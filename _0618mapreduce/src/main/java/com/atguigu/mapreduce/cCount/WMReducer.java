package com.atguigu.mapreduce.cCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WMReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	IntWritable v = new IntWritable();

	// hello 1
	// hello 1

	// world 1
	// world 1
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		// 1 统计单词总个数
		int sum = 0;

		for (IntWritable count : values) {
			sum += count.get();
		}

		v.set(sum);

		// 2 输出单词总个数
		context.write(key, v);
	}
}
