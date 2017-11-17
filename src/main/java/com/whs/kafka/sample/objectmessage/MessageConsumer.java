package com.whs.kafka.sample.objectmessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

/**
 * Message的消费者
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年1月11日 上午9:55:08
 */
public class MessageConsumer {

	public static void main(String[] args) {
		
		String consumerTopic = "kafka-topic-mesage-sync-1";
		String consumerGroup = "message-group";
		
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.56.101:2181");
		props.put("group.id", consumerGroup);
		props.put("auto.offset.reset", "smallest");
		
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
		
		Map<String, Integer> topicConsumerThreads = new HashMap<>();
		topicConsumerThreads.put(consumerTopic, 4);
		
		//key的解码实现类
		StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
		//Message的解码实现类
		MessageDecoder messageDecoder = new MessageDecoder();
		//获取partition的流
		Map<String, List<KafkaStream<String, Message>>> topicStreamsMap = consumer.createMessageStreams(topicConsumerThreads, keyDecoder, messageDecoder);
		
		List<KafkaStream<String, Message>> streams = topicStreamsMap.get(consumerTopic);
		
		System.out.println("thread count : " + streams.size());
		
		try {
			TimeUnit.SECONDS.sleep(3);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		
		List<Thread> threadList = new ArrayList<>(streams.size());
		int threadCount = 0;
		for (KafkaStream<String, Message> stream : streams) {
			Thread thread = new Thread(new ConsumerThread(stream),"thread_" + (threadCount++));
			thread.start();
			threadList.add(thread);
		}
		
		try {
			for (Thread thread : threadList) {
				thread.join();
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

/**
 * 消费的thread
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年1月11日 下午5:33:36
 */
class ConsumerThread implements Runnable {
	
	private KafkaStream<String, Message> stream;
	
	public ConsumerThread(KafkaStream<String, Message> stream) {
		this.stream = stream;
	}
	
	@Override
	public void run() {
		ConsumerIterator<String, Message> consumerIter = this.stream.iterator();
		while(consumerIter.hasNext()) {
			Message message = consumerIter.next().message();
			System.out.println(Thread.currentThread().getName() + " : " + message.toString() + " : " + consumerIter.next().key());
		}
	}
}
