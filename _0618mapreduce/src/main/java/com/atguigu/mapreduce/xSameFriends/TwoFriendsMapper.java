package com.atguigu.mapreduce.xSameFriends;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TwoFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
	Text k = new Text();
	Text v = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1 获取一行A I,K,C,B,G,F,H,O,D,
		String line = value.toString();

		// 2 切割
		String[] fileds = line.split("\t");

		// 3 获取person和好友
		String friend = fileds[0];
		String[] persons = fileds[1].split(",");

		Arrays.sort(persons);

		v.set(friend);
		// 4写出去
		for (int i = 0; i < persons.length - 1; i++) {
			for (int j = i + 1; j < persons.length; j++) {
				k.set(persons[i] + "-" + persons[j]);
				context.write(k, v);
			}
		}
	}
}
