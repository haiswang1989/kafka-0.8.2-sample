package com.whs.kafka.sample.objectmessage;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

/**
 * 需要发送到Kafka的message
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年1月11日 上午9:52:25
 */

@Getter
@Setter
@ToString
@AllArgsConstructor
public class Message implements Serializable {

	private static final long serialVersionUID = 1L;
	
	//消息Id
	private String messageId;
	
	//消息的内容
	private String message;
}
