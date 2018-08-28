package com.mapreduce.xIndex;

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

public class OneIndexDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		args = new String[] { "F:/demo/inputIndex/in", 
		"F:/demo/inputIndex/outNo" };

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(OneIndexDriver.class);
		job.setMapperClass(OneIndexMapper.class);
		job.setReducerClass(OneIndexReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/**********要将自定义的OutputFormat设置到job中**********/
		job.setOutputFormatClass(OneIndexOutputformat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);
	}
}
