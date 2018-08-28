package com.atguigu.mapreduce.xIndex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*	atguigu pingping
	atguigu ss
	atguigu ss
	
	atguigu--a.txt	3
	atguigu--b.txt	2
	atguigu--c.txt	2
	pingping--a.txt	1
	pingping--b.txt	3
	pingping--c.txt	1
	ss--a.txt	2
	ss--b.txt	1
	ss--c.txt	1
	
	atguigu	c.txt--2	b.txt--2	a.txt--3	
	pingping	c.txt--1	b.txt--3	a.txt--1	
	ss	c.txt--1	b.txt--1	a.txt--2	

*/
public class TwoIndexDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "F:/demo/inputIndex/outOne", 
		"F:/demo/inputIndex/outTwo" };
		// 1 获取job对象
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 设置jar包路径
		job.setJarByClass(TwoIndexDriver.class);
		job.setMapperClass(TwoIndexMapper.class);
		job.setReducerClass(TwoIndexReducer.class);

		// 4 设置mapper输出的kv类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// 5 设置最终输出kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		
		// 6 设置输入输出路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		// 7 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
