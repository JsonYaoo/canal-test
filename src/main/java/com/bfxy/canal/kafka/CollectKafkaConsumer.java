package com.bfxy.canal.kafka;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class CollectKafkaConsumer {

	private final KafkaConsumer<String, String> consumer;
	
	private final String topic;
	
	public CollectKafkaConsumer(String topic) { 
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.11.221:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-group-id"); 
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); 
//		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		//	
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");//"latest"
		//latest,earliest 
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000"); 
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"); 
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer"); 
		consumer = new KafkaConsumer<>(props); 
		this.topic = topic; 
		//	订阅主题
		consumer.subscribe(Collections.singletonList(topic));
	} 
	
    private void receive(KafkaConsumer<String, String> consumer) {
        while (true) {
            // 	拉取结果集
        	ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            
            for (TopicPartition partition : records.partitions()) {
                List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                String topic = partition.topic();
                int size = partitionRecords.size();
                //	获取topic: test-db.demo, 分区位置: 2, 消息数为:1
                System.err.println("获取topic: " + topic + ", 分区位置: " + partition.partition() + ", 消息数为:" + size);
            
                for (int i = 0; i< size; i++) {
                	/**
                	 * {
                	 *   ----> "data":[{"id":"010","name":"z100","age":"35"}],
                	 *   ----> "database":"test-db",
                	 *   "es":1605269364000,
                	 *   ----> "id":2,
                	 *   "isDdl":false,
                	 *   ----> "mysqlType":{"id":"varchar(32)","name":"varchar(40)","age":"int(8)"},
                	 *   ----> "old":[{"name":"z10","age":"32"}],
                	 *   ----> "pkNames":["id"],
                	 *   "sql":"",
                	 *   "sqlType":{"id":12,"name":12,"age":4},
                	 *   ----> "table":"demo",
                	 *   ----> "ts":1605269365135,
                	 *   ----> "type":"UPDATE"}
                	 */
                	System.err.println("-----> value: " + partitionRecords.get(i).value());
                    long offset = partitionRecords.get(i).offset() + 1;
//                	consumer.commitSync();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset)));
                    System.err.println("同步成功, topic: " + topic+ ", 提交的 offset: " + offset);
                }
                //	
                //System.err.println("msgList: " + msgList);
            }
        }
    }

	public static void main(String[] args) {
		String topic = "test-db.demo";
		CollectKafkaConsumer collectKafkaConsumer = new CollectKafkaConsumer(topic);
		collectKafkaConsumer.receive(collectKafkaConsumer.consumer);
	}
}
