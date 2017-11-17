package com.whs.kafka.sample.objectmessage;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import kafka.serializer.Decoder;

/**
 * 
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年1月11日 下午5:03:15
 */
public class MessageDecoder implements Decoder<Message> {
	
	@Override
	public Message fromBytes(byte[] bytes) {
		ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
		
		Message msgObj = null;
		
		try(ObjectInputStream ois = new ObjectInputStream(bais)) {
			Object obj = ois.readObject();
			if(obj instanceof Message) {
				msgObj = (Message)obj;
			}
		} catch (Exception e) {
		}
		
		return msgObj;
	}
	
}
