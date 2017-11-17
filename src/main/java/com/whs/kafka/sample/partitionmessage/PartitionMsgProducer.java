package com.whs.kafka.sample.partitionmessage;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class PartitionMsgProducer {
	
	public static void main(String[] args) {
		
		String topic = "kafka-topic-partition-1";
		
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.56.101:9092");
		//设置自定义分区函数
		props.put("partitioner.class", "com.whs.kafka.sample.partitionmessage.MyPartitioner");
		//消息体的序列化方式
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		long t1 = System.currentTimeMillis();
		System.out.println("start : " + t1);
		for(int i=0; i<10000; i++) {
			String key = i + "";
			String message = key;
			KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, key, message);
			producer.send(msg);
		}
		long t2 = System.currentTimeMillis();
		System.out.println("use " + (t2-t1) + "(ms)");
		
		producer.close();
	}
}
