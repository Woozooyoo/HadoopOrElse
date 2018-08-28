package com.mapreduce.xIndex;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneIndexOutputformat extends FileOutputFormat<Text, IntWritable>{

	@Override
	public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		// 创建一个RecordWriter
		return new OneIndexRecordWriter(job);
	}
}
