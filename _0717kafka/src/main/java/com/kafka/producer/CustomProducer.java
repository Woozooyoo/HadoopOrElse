package com.kafka.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class CustomProducer {
	public static void main(String[] args) {
		Properties props = new Properties();
		// Kafka服务端的主机名和端口号
		props.put("bootstrap.servers", "hadoop102:9092");
		// 等待所有副本节点的应答,有一个节点不应答,则不可见
		props.put("acks", "all");
		// 消息发送最大尝试次数
		props.put("retries", 0);
		// 一批消息处理大小
		props.put("batch.size", 16384);
		// 请求延时
		props.put("linger.ms", 1);
		// 发送缓存区内存大小
		props.put("buffer.memory", 33554432);
		// key序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		//为Producer注册拦截器
		/*List<String> interceptorList = new ArrayList<>();
		interceptorList.add("com.kafka.interceptor.TimeInterceptor");
		interceptorList.add("com.kafka.interceptor.CounterInterceptor");
		
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorList);*/
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		/*  （1）指定了partition，则直接使用；
			（2）未指定partition但指定key，通过对key的value进行hash出一个partition；
			（3）partition和key都未指定，使用轮询选出一个partition
			*/
		for (int i = 0; i < 50; i++) {
			//如果不设置 key默认为null
			producer.send(new ProducerRecord<> ("first", Integer.toString (i), "hello world-" + i));
		}

		producer.close();
	}
}

