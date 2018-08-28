package com.mr2;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * @OutPut param ImmutableBytesWritable 理解为RowKey
 * @OutPut param Put：用于封装待存放的数据
 */
public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	/**
	 * @Input param LongWritable：offset偏移量
	 * @Input param Text：一行数据
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		// 从 HDFS 中读取的数据
		String lineValue = value.toString();
		
		// 读取出来的每行数据使用\t 进行分割，存于 String 数组
		String[] values = lineValue.split("\t");

		// 根据数据中值的含义取值
		byte[] rowKey = Bytes.toBytes(values[0]);
		
		byte[] name = Bytes.toBytes(values[1]);
		
		byte[] color = Bytes.toBytes(values[2]);

		// 初始化 rowKey
		ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(rowKey);

		// 初始化 put 对象
		Put put = new Put(rowKey);

		// 参数分别:列族、列、值
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), name);
		
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), color);
		
		context.write(rowKeyWritable, put);
		
	}
}
