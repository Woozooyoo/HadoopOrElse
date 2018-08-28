package com.mapreduce.weblogEtlComplex2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FilterOutputformat extends FileOutputFormat<WebLogBean, NullWritable>{

	@Override
	public RecordWriter<WebLogBean, NullWritable> getRecordWriter(TaskAttemptContext job)
			throws IOException, InterruptedException {
		
		// 创建一个RecordWriter
		return new FilterRecordWriter(job);
	}
}
