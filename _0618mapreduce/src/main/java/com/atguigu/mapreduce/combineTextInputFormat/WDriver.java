package com.atguigu.mapreduce.combineTextInputFormat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*	hello.txt
	hello1.txt
	hello2.txt
	hello3.txt
	hello4.txt*/
public class WDriver {
	public static void main(String[] args) throws IOException, Exception, InterruptedException {
		args = new String[]{"D:/Soft/DevSoft/code/demo/inputword/in",
				"D:/Soft/DevSoft/code/demo/inputword/outCombineTextInputFormat"};

		Configuration configuration = new Configuration ();
		Job job = Job.getInstance (configuration);

		job.setJarByClass (WDriver.class);
		job.setMapperClass (WMapper.class);
		job.setReducerClass (WMReducer.class);

		job.setMapOutputKeyClass (Text.class);
		job.setMapOutputValueClass (IntWritable.class);
		job.setOutputKeyClass (Text.class);
		job.setOutputValueClass (IntWritable.class);

		/**********8 设置读取输入文件切片的类**********/
		job.setInputFormatClass (CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize (job, 4194304);
		CombineTextInputFormat.setMinInputSplitSize (job, 2097152);

		FileInputFormat.setInputPaths (job, new Path (args[0]));
		FileOutputFormat.setOutputPath (job, new Path (args[1]));

		boolean result = job.waitForCompletion (true);
		System.exit (result ? 0 : 1);

	}
}
