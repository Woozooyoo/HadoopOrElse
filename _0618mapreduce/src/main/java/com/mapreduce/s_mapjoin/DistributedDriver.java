package com.mapreduce.s_mapjoin;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DistributedDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "F:/demo/inputMapjoinCache/in", 
		"F:/demo/inputMapjoinCache/out" };

		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);

		job.setJarByClass(DistributedDriver.class);
		job.setMapperClass(DistributedMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		/**********6 加载缓存数据**********/
		job.addCacheFile(new URI("file:/f:/demo/inputCache/inputCache/pd.txt"));
		
		/**********7 map端join的逻辑不需要reduce阶段，设置reducetask数量为0&*/
		job.setNumReduceTasks(0);

		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
