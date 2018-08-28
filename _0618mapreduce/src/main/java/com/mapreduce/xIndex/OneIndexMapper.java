package com.mapreduce.xIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class OneIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	Text k = new Text();
	IntWritable v = new IntWritable(1);
	String name;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		// 0 获取输入文件类型
		FileSplit inputSplit = (FileSplit) context.getInputSplit();
		name = inputSplit.getPath().getName();
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 1 一行内容转换成string
		String line = value.toString();
		
		// 2 切割
		String[] words = line.split(" ");
		
		// 3 循环写出到下一个阶段
		for (String word : words) {
			
			k.set(word + "--" + name);
			
			context.write(k, v);
			
		}
		
	}

}
