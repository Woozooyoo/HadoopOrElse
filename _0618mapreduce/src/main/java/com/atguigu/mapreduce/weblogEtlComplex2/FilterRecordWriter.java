package com.atguigu.mapreduce.weblogEtlComplex2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FilterRecordWriter extends RecordWriter<WebLogBean, NullWritable>{
	
	private FSDataOutputStream trueOut = null;
	private FSDataOutputStream otherOut = null;
	
	public FilterRecordWriter(TaskAttemptContext job)  {
		Configuration configuration = job.getConfiguration();
		
		try {
			// 获取文件系统
			FileSystem fs = FileSystem.get(configuration);
			
			// 创建两个文件的输出流
			trueOut = fs.create(new Path("F:/demo/inputETLweblog/outComplex2/trueOut.log"));
			
			otherOut = fs.create(new Path("F:/demo/inputETLweblog/outComplex2/other.log"));
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}

	@Override
	public void write(WebLogBean key, NullWritable value) throws IOException, InterruptedException {
		// 区分输入的key是否包含atguigu
		
		if (key.isValid()) {// 包含
			trueOut.write(key.toString().getBytes());
		}else {// 不包含
			otherOut.write(key.toString().getBytes());
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		
		if (trueOut != null) {
			trueOut.close();
		}
		
		if (otherOut != null) {
			otherOut.close();
		}
	}

}
