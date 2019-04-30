package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Properties;

import kafka.coordinator.BaseKey;
import kafka.coordinator.GroupMetadata;
import kafka.coordinator.GroupMetadataKey;
import kafka.coordinator.GroupMetadataManager;
import kafka.coordinator.GroupTopicPartition;
import kafka.coordinator.OffsetKey;

/**
 * __consumer_offsets topic 消费
 * kafka新版本 大量消息写入从zk 转移到了 kafka, 写入__consumer_offsets 这个公共topic中
 * __consumer_offsets topic的消息格式：[Group, Topic, Partition]::[OffsetMetadata[Offset, Metadata], CommitTime, ExpirationTime]
 * 该topic的元数据信息有两种:
 *    1. key：OffsetKey, value:OffsetAndMetadata 保存了消费者组各个partition的offset位移信息元数据
 *    2. key: GroupMetadataKey, value: GroupMetadata 保存了消费者组中各个消费者的信息
 * @author beginman 2019-04-30 14:51
 **/
public class MetaDataConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();

        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test_interval_offset");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("value.deserializer",
                "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props);
        consumer.subscribe(Collections.singletonList("__consumer_offsets"));

        while (true) {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(100);
            for (ConsumerRecord<byte[], byte[]> record: records) {
                // 对key进行解析，OffsetKey、GroupMetaDataKey
                BaseKey key = GroupMetadataManager.readMessageKey(ByteBuffer.wrap(record.key()));
                if (key instanceof OffsetKey) {
                    GroupTopicPartition partition = (GroupTopicPartition) key.key();
                    String topic = partition.topicPartition().topic();
                    String group = partition.group();
                    // key.toString(), 返回：[group, topic, partition]
                    System.out.println(">>> OffsetKey  group: " + group + " topic: " + topic + " key: " + key.toString());
                } else if (key instanceof GroupMetadataKey) {
                    GroupMetadata groupMetadata = GroupMetadataManager.readGroupMessageValue((
                            (GroupMetadataKey) key).key(), ByteBuffer.wrap(record.value()));
                    System.out.println("--- GroupMetadataKey: " + key.key() + " GroupMetadata: " + groupMetadata.toString());
                }

                // 对value进行解析
                GroupMetadataManager.OffsetsMessageFormatter formatter = new GroupMetadataManager.OffsetsMessageFormatter();
                formatter.writeTo(record, System.out);
                System.out.println("");

                // out
                /**
                 * --- GroupMetadataKey: single-consumer-group GroupMetadata: [single-consumer-group,Some(consumer),Stable,Map(consumer-1-228b379d-a124-4e79-bd58-8724d5419c10 -> [consumer-1-228b379d-a124-4e79-bd58-8724d5419c10,consumer-1,/127.0.0.1,10000])]
                 *
                 * >>> OffsetKey  group: single-consumer-group topic: demo key: [single-consumer-group,demo,3]
                 * [single-consumer-group,demo,3]::[OffsetMetadata[0,NO_METADATA],CommitTime 1556608315876,ExpirationTime 1556694715876]
                 *
                 * >>> OffsetKey  group: single-consumer-group topic: demo key: [single-consumer-group,demo,4]
                 * [single-consumer-group,demo,4]::[OffsetMetadata[0,NO_METADATA],CommitTime 1556608315876,ExpirationTime 1556694715876]
                 *
                 * >>> OffsetKey  group: single-consumer-group topic: demo key: [single-consumer-group,demo,0]
                 * [single-consumer-group,demo,0]::[OffsetMetadata[0,NO_METADATA],CommitTime 1556608315876,ExpirationTime 1556694715876]
                 *
                 */
            }
        }

    }
}
