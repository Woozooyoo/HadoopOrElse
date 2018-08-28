package com.mr1;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
/**
 * @OutPut param ImmutableBytesWritable 理解为RowKey
 * @OutPut param Put：用于封装待存放的数据
 */
public class ReadFruitMapper extends TableMapper<ImmutableBytesWritable/* rowKey */, Put> {

	/**
	 * @Input param ImmutableBytesWritable 理解为RowKey
	 * @Input param Result：每一个该类型的实例化对象，都对应了一个rowkey中的若干数据
	 */
	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		
		// 将fruit的name和color提取出来，相当于将每一行数据读取出来放入到Put对象中
		Put put = new Put(key.get());
		
		// 遍历添加column行 	//result.rawCells(); ==> a cell
		for (Cell cell : value.rawCells()) {
			
			// 添加/克隆列族:info
			if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
				
				// 添加/克隆列：name
				if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					// 将该列cell加入到put对象中
					put.add(cell);
					
					// 添加/克隆列:color
				}/* else if ("color".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
					// 向该列cell加入到put对象中
					put.add(cell);
				}*/
			}
		}

		// 将从fruit读取到的每行数据写入到context中作为map的输出
		context.write(key, put);
	}
}
