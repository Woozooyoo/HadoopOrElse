package com.mapreduce.xIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TwoIndexReducer extends Reducer<Text, Text, Text, Text>{
	
	Text k = new Text();
	Text v = new Text();

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		StringBuilder sb = new StringBuilder();
		
		// 1 累加求和
		for (Text value: values) {
			sb.append(value.toString().replace("\t", "--") + "\t");
		}
		
		
		v.set(sb.toString());
		// 2 输出
		context.write(key,v);
	}
}
