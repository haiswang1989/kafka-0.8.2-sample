package com.kafka08.sample.partitionmessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

public class PartitionMsgConsumer {
	public static void main(String[] args) {
		String consumerGroup = "partition-group";
		String consumerTopic = "kafka-topic-partition-1";
		
		Properties props = new Properties();
		//zookeeper配置
		props.put("zookeeper.connect", "192.168.56.101:2181");
		//消费组
		props.put("group.id", consumerGroup);
		
		props.put("auto.offset.reset", "smallest");
		
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		topicCountMap.put(consumerTopic, 8);
		
		Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		List<KafkaStream<byte[],byte[]>> streams = consumerMap.get(consumerTopic);
		
		List<Thread> threadList = new ArrayList<Thread>();
		int threadCount = 0;
		
		for (final KafkaStream<byte[],byte[]> kafkaStream : streams) {
			Thread thread = new Thread(new ConsumerThread(kafkaStream, consumer), "Thread _" + (++threadCount));
			thread.start();
			threadList.add(thread);
		}
		
		for (Thread thread : threadList) {
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		consumer.shutdown();
		System.out.println("over...");
	}
}

class ConsumerThread implements Runnable {
	
	private KafkaStream<byte[],byte[]> stream;
	private ConsumerConnector consumer;
	
	public ConsumerThread(KafkaStream<byte[],byte[]> stream, ConsumerConnector consumer) {
		this.stream = stream;
		this.consumer = consumer;
	}

	public void run() {
		ConsumerIterator<byte[], byte[]> iter = stream.iterator();
		while(iter.hasNext()) {
			String message = new String(iter.next().message());
			String key = new String(iter.next().key());
			System.out.println(Thread.currentThread().getName() + " : " + message + " : " + key);
//			consumer.commitOffsets();
		}
		
		//提交offset
		//consumer.commitOffsets();
		//consumer.commitOffsets(true);
	}
}
