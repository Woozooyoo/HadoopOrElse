package com.atguigu.mapreduce.xSameFriends;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class OneRecordWriter extends RecordWriter<Text, Text>{
	
	private FSDataOutputStream trueOut = null;
	
	public OneRecordWriter(TaskAttemptContext job)  {
		Configuration configuration = job.getConfiguration();
		
		try {
			// 获取文件系统
			FileSystem fs = FileSystem.get(configuration);
			
			// 创建两个文件的输出流
			trueOut = fs.create(new Path("D:/Soft/DevSoft/code/demo/inputxSameFriends/outOne/oneOut.log"));
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, Text value) throws IOException, InterruptedException {
			
			trueOut.write((key.toString()+"\t"+value.toString()+"\r\n").getBytes());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		
		if (trueOut != null) {
			trueOut.close();
		}
		
	}

}
