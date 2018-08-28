package com.atguigu.mapreduce.xIndex;

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

public class OneIndexRecordWriter extends RecordWriter<Text, IntWritable>{
	
	private FSDataOutputStream trueOut = null;
	
	public OneIndexRecordWriter(TaskAttemptContext job)  {
		Configuration configuration = job.getConfiguration();
		
		try {
			// 获取文件系统
			FileSystem fs = FileSystem.get(configuration);
			
			// 创建两个文件的输出流
			trueOut = fs.create(new Path("F:/demo/inputIndex/outOne/trueOut.log"));
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

	@Override
	public void write(Text key, IntWritable value) throws IOException, InterruptedException {
		// 区分输入的key是否包含atguigu
		
//			int sum =0;
			
//			// 1 累加求和
//			for (IntWritable count : value) {
//				sum+=count.get();
//			}
			
			trueOut.write((key.toString()+value.toString()+"\r\n").getBytes());
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		
		if (trueOut != null) {
			trueOut.close();
		}
		
	}

}
