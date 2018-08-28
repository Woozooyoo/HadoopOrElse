package com.mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/** 具体读文件的类*/
public class WholeRecordReader extends RecordReader<Text, BytesWritable>{
	private BytesWritable value = new BytesWritable();
	private Text k = new Text();
	private boolean isProcess = true;
	private FileSplit split;
	private Configuration configuration;
		
	// 初始化方法
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// 获取切片信息
		this.split = (FileSplit) split;
		
		// 获取配置信息
		configuration = context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// 读取一个一个的文件
		if (isProcess) {
			// 0 缓存区
			byte[] buf = new byte[(int) split.getLength()];
			
			FileSystem fs = null;
			
			FSDataInputStream fis = null;
			
			try {
				// 1 获取文件系统
				// 获取切片的路径
				Path path = split.getPath();
				fs = path.getFileSystem(configuration);
				
				// 2 打开文件输入流
				fis = fs.open(path);
				
				// 3 流的拷贝 读取数据
				IOUtils.readFully(fis, buf, 0, buf.length);
				
				// 4 拷贝缓冲区的数据到最终输出
				value.set(buf, 0, buf.length);

				// 设置k
				k.set(path.toString());
				
			} finally {
				IOUtils.closeStream(fis);
				IOUtils.closeStream(fs);
			}
			
			isProcess = false;
			
			return true;
		}
		
		return false;
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return k;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return isProcess? 1:0;
	}

	@Override
	public void close() throws IOException {

	}

}
