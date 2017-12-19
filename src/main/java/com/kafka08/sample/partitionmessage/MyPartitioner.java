package com.kafka08.sample.partitionmessage;

import java.util.Random;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

/**
 * 自定义分区函数
 * <p>Description:</p>
 * @author hansen.wang
 * @date 2017年1月11日 下午5:52:47
 */
public class MyPartitioner implements Partitioner {
	
	Random random = null;
	
	public MyPartitioner(VerifiableProperties properties) {
		random = new Random(System.currentTimeMillis());
	}
	
	/**
	 * 分区的key,topic的Partition的数量
	 */
	@Override
	public int partition(Object partitionKey, int partitionCount) {
		//随机分发
		return random.nextInt(partitionCount);
	}

}
