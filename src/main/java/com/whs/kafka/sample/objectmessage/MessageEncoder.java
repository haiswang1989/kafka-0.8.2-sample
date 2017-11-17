package com.whs.kafka.sample.objectmessage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import kafka.serializer.Encoder;
import kafka.utils.VerifiableProperties;

/**
 * Message的编码
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年1月11日 上午10:12:13
 */
public class MessageEncoder implements Encoder<Message> {
	
	/**
	 * 该构造方法必须有,否则会报错
	 * @param properties
	 */
    public MessageEncoder(VerifiableProperties properties) {
    }
	
	public byte[] toBytes(Message mesage) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try(ObjectOutputStream oos = new ObjectOutputStream(baos)) {
			oos.writeObject(mesage);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return baos.toByteArray();
	}
}
