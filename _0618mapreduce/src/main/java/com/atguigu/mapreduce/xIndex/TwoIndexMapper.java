package com.atguigu.mapreduce.xIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TwoIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text k = new Text();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 一行内容转换成string
		String line = value.toString();

		// 2 切割
		String[] words = line.split("--");

		k.set(words[0]);

		v.set(words[1]);

		context.write(k, v);

	}

}
