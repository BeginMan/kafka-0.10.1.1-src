/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import kafka.utils.timer._

import scala.collection._

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
  * Kafka将一些不立即执行而要等待满足一定条件之后才触发完成的操作称为延迟操作
  * DelayedOperation 抽象类
  * 基于一个事件启动有失效时间的TimerTask
 */
abstract class DelayedOperation(override val delayMs: Long) extends TimerTask with Logging {

  // 控制某个延迟操作
  private val completed = new AtomicBoolean(false)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   *
   * 该方法在条件满足时，检测延迟任务是否未被执行。
   * 若未被执行，则先调用TimerTask.cancel()方法解除该延迟操作与TimerTaskEntry的绑定，将该延迟操作从TimerTaskList链表中移除，
   * 然后调用onComplete()方法让延迟操作执行完成。
   * 通过completed的CAS原子操作（completed.compareAndSet），可以保证并发操作时只有第一个调用该方法的线程能够顺利调用onComplete()完成延迟操作，
   * 其他线程获取的completed属性为false，即不会调用onComplete()方法，这就保证了onComplete()只会被调用一次。
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   */
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
    * 也是一个抽象方法，由子类来实现当延迟操作已达失效时间的相应逻辑处理。
    *
    * Kafka通过SystemTimer来定期检测请求是否超时。
    * SystemTimer是Kafka实现的底层基于层级时间轮和DelayQueue定时器，维护了一个newFixedThreadPool线程池，用于提交相应的线程执行。
    * 例如，当检测到延迟操作已失效时则将延迟操作提交到该线程池，即执行线程的run()方法的逻辑。
    * DelayedOperation覆盖了TimerTask的run()方法，在该方法中先调用forceCompete()方法，当该方法返回true后再调用onExpiration()方法。
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
    *
    * 在延迟时间(delayMs）内，onComplete()方法只被调用一次
    * 是一个抽象方法，由子类来实现，执行延迟操作满足执行条件后需要执行的实际业务逻辑。
    * 例如，DelayedProduce和DelayedFetch都是在该方法内调用responseCallback向客户端做出响应。
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
    * 一个抽象方法，由子类来实现，负责检测执行条件是否满足。若满足执行条件，则调用forceComplete()方法完成延迟操作。
    */
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete(). This can be overridden if the operation provides its
   * own synchronization.
    * 线程安全，以synchronized同步锁调用onComplete()方法，供外部调用
   */
  def safeTryComplete(): Boolean = {
    synchronized {
      tryComplete()
    }
  }

  /*
   * run() method defines a task that is executed on timeout
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                       timeoutTimer: Timer,
                                                       brokerId: Int = 0,
                                                       purgeInterval: Int = 1000,
                                                       reaperEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {

  /* a list of operation watching keys */
  private val watchersForKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

  private val removeWatchersLock = new ReentrantReadWriteLock()

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)

  newGauge(
    "PurgatorySize",
    new Gauge[Int] {
      def value = watched()
    },
    metricsTags
  )

  newGauge(
    "NumDelayedOperations",
    new Gauge[Int] {
      def value = delayed()
    },
    metricsTags
  )

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling
    // tryComplete() for each key is going to be expensive if there are many keys. Instead,
    // we do the check in the following way. Call tryComplete(). If the operation is not completed,
    // we just add the operation to all keys. Then we call tryComplete() again. At this time, if
    // the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys. This does mean that
    // if the operation is completed (by another thread) between the two tryComplete() calls, the
    // operation is unnecessarily added for watch. However, this is a less severe issue since the
    // expire reaper will clean it up periodically.

    var isCompletedByMe = operation.safeTryComplete()
    if (isCompletedByMe)
      return true

    var watchCreated = false
    for(key <- watchKeys) {
      // If the operation is already completed, stop adding it to the rest of the watcher list.
      if (operation.isCompleted)
        return false
      watchForOperation(key, operation)

      if (!watchCreated) {
        watchCreated = true
        estimatedTotalOperations.incrementAndGet()
      }
    }

    isCompletedByMe = operation.safeTryComplete()
    if (isCompletedByMe)
      return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    if (!operation.isCompleted) {
      timeoutTimer.add(operation)
      if (operation.isCompleted) {
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   */
  def checkAndComplete(key: Any): Int = {
    val watchers = inReadLock(removeWatchersLock) { watchersForKey.get(key) }
    if(watchers == null)
      0
    else
      watchers.tryCompleteWatched()
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched() = allWatchers.map(_.countWatched).sum

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def delayed() = timeoutTimer.size

  /*
   * Return all the current watcher lists,
   * note that the returned watchers may be removed from the list by other threads
   */
  private def allWatchers = inReadLock(removeWatchersLock) { watchersForKey.values }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T) {
    inReadLock(removeWatchersLock) {
      val watcher = watchersForKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers) {
    inWriteLock(removeWatchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (watchersForKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        watchersForKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown() {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: T) {
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          iter.remove()
        } else if (curr.safeTryComplete()) {
          iter.remove()
          completed += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    // traverse the list and purge elements that are already completed by others
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long) {
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    if (estimatedTotalOperations.get - delayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      estimatedTotalOperations.getAndSet(delayed)
      debug("Begin purging watch lists")
      val purged = allWatchers.map(_.purgeCompleted()).sum
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d".format(brokerId),
    false) {

    override def doWork() {
      advanceClock(200L)
    }
  }
}
