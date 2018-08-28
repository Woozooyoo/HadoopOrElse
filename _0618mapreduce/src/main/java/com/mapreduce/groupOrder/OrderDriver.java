package com.mapreduce.groupOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OrderDriver {
  /*0000001	Pdt_01	222.8
	0000002	Pdt_05	722.4
	0000001	Pdt_05	25.8
	0000003	Pdt_01	222.8
	0000003	Pdt_01	33.8
	0000002	Pdt_03	522.8
	0000002	Pdt_04	122.4
	
	3	222.8
	*/
	public static void main(String[] args) throws Exception {
		args = new String[] { "F:/demo/inputGroupingComparator/in", 
		"F:/demo/inputGroupingComparator/outputOrder4" };

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(OrderDriver.class);
		job.setMapperClass(OrderMapper.class);
		job.setReducerClass(OrderReducer.class);

		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		/**********10 关联groupingComparator**********/
		job.setGroupingComparatorClass(OrderGroupingCompartor.class);
		
		/** 7 设置分区*/
		job.setPartitionerClass(OrderPatitioner.class);

		/**8 设置reduce个数*/
		job.setNumReduceTasks(3);
		
		// 9 提交
		boolean result = job.waitForCompletion(true);
		System.exit(result ? 0 : 1);

	}
}
