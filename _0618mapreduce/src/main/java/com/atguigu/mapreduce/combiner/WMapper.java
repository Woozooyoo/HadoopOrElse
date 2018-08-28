package com.atguigu.mapreduce.combiner;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * 输入的key  LongWritable   行号
 * 输入的value Text			一行内容
 * 输出的key Text				单词
 * 输出的value IntWritable	单词的个数
 */
public class WMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	Text k = new Text();
	IntWritable v = new IntWritable(1);
//	hello world
//	atguigu atguigu
//	hadoop 
//	spark
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 1 一行内容转换成string
		String line = value.toString();
		
		// 2 切割
		String[] words = line.split(" ");
		
		// 3 循环写出到下一个阶段
		for (String word : words) {
			
			k.set(word);
			
			context.write(k, v);
			
		}
		
	}

}
