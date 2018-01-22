package com.kafka10.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * kafka在0.10.0.1版本引入的消息的时间戳
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2018年1月22日 上午10:59:18
 */
public class Kafka10TimestampProducer {
    
    //消息含有时间戳的topic
    private static final String TOPIC = "TOPIC_TIMESTAMP_MSG";
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        Properties properties = new Properties();
        //broker集群的地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.103:9092");
        //key的序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //value的序列化方式
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        
        boolean isSync = true;
        //1516600774730
        //1516600820502
        //1516600885965
        //1516601190190
        long timestamp = System.currentTimeMillis();
        ProducerRecord<String, String> timestampRecord = new ProducerRecord<String, String>(TOPIC, null, timestamp, null, "TIMESTAMP_MSG_1_" + timestamp);
        System.out.println("Send : " + timestampRecord);
        if(isSync) {
            //同步的方式,注意这边同步的方式,需要调用一下Future的get()方法,不然不会立即发送
            producer.send(timestampRecord).get();
        } else {
            //异步的方式
            producer.send(timestampRecord, new ProducerCallback());
        }
        
        producer.close();
        
    }
}
