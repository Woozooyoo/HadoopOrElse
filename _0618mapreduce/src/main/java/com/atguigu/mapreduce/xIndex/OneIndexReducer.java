package com.atguigu.mapreduce.xIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OneIndexReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	Text k = new Text();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		k.set(key.toString() + "\t");
		
		int sum =0;
		
		// 1 累加求和
		for (IntWritable count : values) {
			sum+=count.get();
		}
		
		
		// 2 输出
		context.write(k,new IntWritable(sum) );
	}
}
