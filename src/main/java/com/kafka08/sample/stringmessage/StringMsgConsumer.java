package com.kafka08.sample.stringmessage;

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

public class StringMsgConsumer {

	public static void main(String[] args) {
		
		String consumerGroup = "sync-group";
		String consumerTopic = "kafka-topic-sync-1";
		
		Properties props = new Properties();
		//zookeeper配置
		props.put("zookeeper.connect", "192.168.56.101:2181");
		//消费组
		props.put("group.id", consumerGroup);
		//连接zookeeper的超时时间
		props.put("zookeeper.session.timeout.ms", "6000");
		//zookeeper集群中leader和follower之间的同步时间
		props.put("zookeeper.sync.time.ms", "200");
		
		
//		//consumer在消费数据的时候,offset自动提交到zookeeper,与auto.commit.interval.ms结合使用
//		props.put("auto.commit.enable", "true");
//		//consumer消费数据自动提交offset的时间间隔
//		props.put("auto.commit.interval.ms", "1000");
		
		props.put("auto.offset.reset", "smallest");
		
//		//设置rebalance的尝试次数,已经时间间隔 rebalance.max.retries * rebalance.backoff.ms > zookeeper.session.timeout.ms
//		//设置该参数主要是为了避免在rebalance出现"kafka.common.ConsumerRebalanceFailedException"异常
//		props.put("rebalance.max.retries", "10");  
//		props.put("rebalance.backoff.ms", "1200"); 
		
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
		
		//每个topic分配读取线程数,这些线程就会平均分配读取partition的数据
		Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
		//使用四个线程去消费指定topic的数据(kafka-topic-sync-1有8个partition,这样每个线程去消费2个partition的数据)
		topicCountMap.put(consumerTopic, 3);
		
		Map<String,List<KafkaStream<byte[],byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
		//获得指定topic的流通道
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
			System.out.println(Thread.currentThread().getName() + " : " + new String(iter.next().message()));
		}
		//提交offset
		//consumer.commitOffsets();
		consumer.commitOffsets(true);
	}
}
