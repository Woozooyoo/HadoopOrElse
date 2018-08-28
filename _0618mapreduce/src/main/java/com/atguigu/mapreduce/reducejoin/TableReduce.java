package com.atguigu.mapreduce.reducejoin;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TableReduce extends Reducer<Text, TableBean, TableBean, NullWritable> {

	@Override
	protected void reduce(Text key, Iterable<TableBean> values, Context context)
			throws IOException, InterruptedException {
		
		// 0 准备存储数据的缓存
		TableBean pdbean = new TableBean();
		ArrayList<TableBean> orderBeans = new ArrayList<>();

		// 根据文件的不同分别处理数据

		for (TableBean value : values) {

			if ("0".equals(value.getFlag())) {// 订单表数据处理
				
				TableBean tableBean = new TableBean();

				try {
					BeanUtils.copyProperties(tableBean, value);
				} catch (Exception e) {
					e.printStackTrace();
				}

				orderBeans.add(tableBean);
//				orderBeans.add(value);	//wrong

			} else {// 产品表处理 01 小米
				try {
					BeanUtils.copyProperties(pdbean, value);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		// 数据拼接
		for (TableBean bean : orderBeans) {
			// 更新产品名称字段
			bean.setPname(pdbean.getPname());
			
			// 写出
			context.write(bean, NullWritable.get());
		}
	}
}
