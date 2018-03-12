package com.kafka10.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class Kafka10Producer {

    private static final String TOPIC = "HelloKafka";
    
    public static void main(String[] args) throws InterruptedException, ExecutionException {
        
        Properties properties = new Properties();
        //broker集群的地址
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.102:9092");
        //key的序列化方式
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //value的序列化方式
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        boolean isSync = false;
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "Hello kafka");
        System.out.println("Send : " + record);
        if(isSync) {
            //同步的方式,注意这边同步的方式,需要调用一下Future的get()方法,不然不会立即发送
            producer.send(record).get();
        } else {
            //异步的方式
            producer.send(record, new ProducerCallback());
            //异步发送如果需要立即发送到server端,这边需要flush一下,不然不会立即发送到server端
            producer.flush();
        }
        
        producer.close();
    }
}


/**
 * 异步回调
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年12月20日 上午9:42:29
 */
class ProducerCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata arg0, Exception arg1) {
        System.out.println("RecordMetadata : " + arg0);
        System.out.println("Exception : " + arg1);
    }
}