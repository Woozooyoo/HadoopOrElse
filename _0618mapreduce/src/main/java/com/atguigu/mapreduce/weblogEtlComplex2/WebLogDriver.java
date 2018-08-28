package com.atguigu.mapreduce.weblogEtlComplex2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WebLogDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "F:/demo/inputETLweblog/in", 
		"F:/demo/inputETLweblog/outComplex2" };

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(WebLogDriver.class);
		job.setMapperClass(WebLogMapper.class);
		job.setReducerClass(WebLogReduce.class);

		job.setMapOutputKeyClass(WebLogBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		/**********要将自定义的OutputFormat设置到job中**********/
		job.setOutputFormatClass(FilterOutputformat.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
