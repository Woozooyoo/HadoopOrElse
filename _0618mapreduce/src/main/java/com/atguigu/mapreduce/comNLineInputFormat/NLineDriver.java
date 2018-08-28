package com.atguigu.mapreduce.comNLineInputFormat;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class NLineDriver {

	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		args = new String[] { "D:/Soft/DevSoft/code/demo/inputComNLineInputFormat/in",
				"D:/Soft/DevSoft/code/demo/inputComNLineInputFormat/out" };

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		/** 设置每个切片InputSplit中划分三条记录*/
		NLineInputFormat.setNumLinesPerSplit(job, 3);

		/** 使用NLineInputFormat处理记录数*/
		job.setInputFormatClass(NLineInputFormat.class);

		job.setJarByClass(NLineDriver.class);
		job.setMapperClass(NLineMapper.class);
		job.setReducerClass(NLineReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
