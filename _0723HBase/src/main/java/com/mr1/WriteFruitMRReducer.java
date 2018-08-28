package com.mr1;

import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

public class WriteFruitMRReducer extends TableReducer<ImmutableBytesWritable, Put, NullWritable> {
	
	/**
	 * ImmutableBytesWritable 按照rowkey聚合 put
	 * Put：用于封装待存放的数据
	 */
	@Override
	protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context)
			throws IOException, InterruptedException {

		// 读出来的每一行数据写入到fruit_mr表中
		for (Put put : values) {
			context.write(NullWritable.get(), put);
		}
	}
}
