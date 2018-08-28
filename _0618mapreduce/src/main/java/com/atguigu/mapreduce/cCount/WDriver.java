package com.atguigu.mapreduce.cCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WDriver {
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		
		args = new String[] { "D:/Soft/DevSoft/code/demo/inputCombinerTextInputFormat/in", 
		"D:/Soft/DevSoft/code/demo/inputCombinerTextInputFormat/outBasic2" };
		
		// 1 获取job信息
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		// 2 获取jar包位置
		job.setJarByClass(WDriver.class);
		
		// 3 关联自定义的mappper和reducer
		job.setMapperClass(WMapper.class);
		job.setReducerClass(WMReducer.class);
		
		// 4 设置map输出数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 5 设置最终输出数据类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 6 设置数据输入和输出文件路径
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// 7 提交代码
//		job.submit();
		boolean result = job.waitForCompletion(true);
		System.exit(result?0:1);
	}
}
