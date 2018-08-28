package com.mr2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// extends Configured直接用hadoop的conf，没拿HBase的conf，不能使用
public class HDFS2HBaseRunner implements Tool {
	
	private static Configuration conf;

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	/**conf 是 bin/start-dfs.sh时在集群里new 的Configuration对象
	 * 会把  etc/hadoop 的*.site.xml等各种配置信息 加载到conf对象
	 * 当在hadoop集群运行任务时，会把当前集群的conf加载到setConf方法参数里
	 */
	@Override
	public void setConf(Configuration conf) {
		this.conf = HBaseConfiguration.create(conf);
	}

	@Override
	public int run(String[] args) throws Exception {

		// 创建 Job 任务
		Job job = Job.getInstance(conf, this.getClass().getSimpleName());
		
		job.setJarByClass(HDFS2HBaseRunner.class);

		// 设置 Mapper
		job.setMapperClass(ReadFruitFromHDFSMapper.class);
		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		// 设置 Reducer 和 OutputFormat
		TableMapReduceUtil.initTableReducerJob(
				"fruit_mr", 
				WriteFruitMRFromTxtReducer.class, 
				job);

		// InputFormat
		FileInputFormat.addInputPath(job, new Path("hdfs://hadoop102:9000/user/atguigu/input/input_fruit/fruit.tsv"));
		
		// 设置 Reduce 数量，最少 1 个
		job.setNumReduceTasks(1);

		boolean isSuccess = job.waitForCompletion(true);
		
		if (!isSuccess) {
			throw new IOException("Job running with error");
		}

		return isSuccess ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		
		int status = ToolRunner.run(new HDFS2HBaseRunner(), args);
		
		System.exit(status);
	}

}
