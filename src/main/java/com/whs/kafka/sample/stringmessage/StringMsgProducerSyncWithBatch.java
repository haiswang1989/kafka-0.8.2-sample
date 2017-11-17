package com.whs.kafka.sample.stringmessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * 同步发送(实时),批量发送,这样性能比单条发送快很多
 * 批量发送的性能也是比较快的
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年1月10日 上午11:14:09
 */
public class StringMsgProducerSyncWithBatch {
	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put("metadata.broker.list", "192.168.56.101:9092");
		//msg的序列化实现类
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//key的序列化实现类
		props.put("key.serializer.class", "kafka.serializer.StringEncoder");
		
		/**
		 * 控制一个producer的请求怎样才能算完成
		 * 0:不会等待来自borker的ack
		 * 1:等待来自leader的ack
		 * -1:等待所有的broker都复制成功的ack
		 */
		props.put("request.required.acks", "1");
		
		//同步发送
		props.put("producer.type", "sync");
		
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);
		
		String topic = "kafka-topic-sync-1";
		
		long t1 = System.currentTimeMillis();
		System.out.println("start : " + t1);
		
		int batchSize = 2000;
		int currCount = 0;
		List<KeyedMessage<String, String>> messages = new ArrayList<>(batchSize);
		
		for(int i=0; i<10000; i++) {
			KeyedMessage<String, String> msg = new KeyedMessage<String, String>(topic, i + "");
			messages.add(msg);
			if((++currCount) % batchSize == 0) {
				producer.send(messages);
				messages.clear();
				currCount = 0;
			}
		}
		
		if(currCount != 0) {
			producer.send(messages);
			messages.clear();
			currCount = 0;
		}
		
		long t2 = System.currentTimeMillis();
		System.out.println("use : " + (t2-t1) + "ms");
		
		producer.close();
	}
}
