package com.kafka08.sample.objectmessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Message的生产者
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年1月11日 上午9:54:52
 */
public class MessageProducer {

	public static void main(String[] args) {
		Properties props = new Properties();
		//broker的列表
		props.put("metadata.broker.list", "192.168.56.101:9092");
		//消息体的编码
		props.put("serializer.class", "com.whs.kafka.sample.objectmessage.MessageEncoder");
		
		/**
		 * 控制一个producer的请求怎样才能算完成
		 * 0:不会等待来自borker的ack
		 * 1:等待来自leader的ack
		 * -1:等待所有的broker都复制成功的ack
		 */
		props.put("request.required.acks", "-1");
		//同步发送
		props.put("producer.type", "sync");
		
		String topic = "kafka-topic-mesage-sync-1";
		
		ProducerConfig producerConf = new ProducerConfig(props);
		Producer<String, Message> messageProducer = new Producer<String, Message>(producerConf);
		
		long t1 = System.currentTimeMillis();
		System.out.println("start : " + t1);
		
		//batch大小
		int batchSize = 2000;
		//当前List中元素的个数
		int count = 0;
		List<KeyedMessage<String, Message>> messages = new ArrayList<>(batchSize);
		
		for(int i=0; i<10000; i++) {
			KeyedMessage<String, Message> msg = new KeyedMessage<String, Message>(topic, new Message(i + "", i + ""));
			messages.add(msg);
			//批量发送
			if((++count) % batchSize == 0) {
				messageProducer.send(messages);
				messages.clear();
				count = 0;
			}
		}
		
		//循环退出以后再发送一次
		if(0 != count) {
			messageProducer.send(messages);
			messages.clear();
			count = 0;
		}
		
		long t2 = System.currentTimeMillis();
		System.out.println("use " + (t2-t1) + "ms");
	}
}
