package com.atguigu.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class CallBackProducer {

	public static void main(String[] args) throws InterruptedException {
		Properties props = new Properties ();
		// Kafka服务端的主机名和端口号
		props.put ("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
		// 等待所有副本节点的应答
		props.put ("acks", "all");
		// 消息发送最大尝试次数
		props.put ("retries", 0);
		// 一批消息处理大小
		props.put ("batch.size", 16384);
		// 增加服务端请求延时
		props.put ("linger.ms", 1);
		// 发送缓存区内存大小
		props.put ("buffer.memory", 33554432);
		// key序列化
		props.put ("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化
		props.put ("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// 自定义分区CustomPartitioner
//		props.put("partitioner.class", "com.atguigu.kafka.partitioner.CustomPartitioner");

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<> (props);

		for (int i = 0; i < 50; i++) {
			Thread.sleep (800);
			//new Callback()回调匿名子类对象
			kafkaProducer.send (new ProducerRecord<String, String> ("first", "hh" + i, "hh" + i), new Callback () {
				@Override
				//回调函数
				public void onCompletion(RecordMetadata metadata, Exception exception) {

					if (metadata != null) {

						System.out.println (metadata.partition () + "---" + metadata.offset ());
					}
				}
			});
		}

		kafkaProducer.close ();

	}

}
