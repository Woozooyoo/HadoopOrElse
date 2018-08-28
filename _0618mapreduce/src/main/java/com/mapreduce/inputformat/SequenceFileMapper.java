package com.mapreduce.inputformat;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable>{

	@Override
	protected void map(Text key, BytesWritable value,
			Context context) throws IOException, InterruptedException {
		// 控制输出数据
		context.write(key, value);
	}

}
