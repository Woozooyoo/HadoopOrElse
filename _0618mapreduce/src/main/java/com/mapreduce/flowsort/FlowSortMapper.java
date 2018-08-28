package com.mapreduce.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlowSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
	
	FlowBean k = new FlowBean();
	Text v = new Text();
	
//	13480253104	180	180	360
//	13502468823	7335	110349	117684
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 1 获取一行
		String line = value.toString();
		
		// 2 切割
		String[] fields = line.split("\t");
		
		// 3 封装对象
		long upFlow = Long.parseLong(fields[1]);
		long downFlow = Long.parseLong(fields[2]);
		
		k.set(upFlow, downFlow);
		v.set(fields[0]);
		
		// 4 写出
		context.write(k, v);
	}

}
