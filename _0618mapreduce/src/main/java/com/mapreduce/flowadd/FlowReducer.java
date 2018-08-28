package com.mapreduce.flowadd;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{

	@Override
	protected void reduce(Text key, Iterable<FlowBean> values, Context context)
			throws IOException, InterruptedException {
		
		// 13560436666   1116		954   2000
		long sum_upFlow = 0;
		long sum_downFlow = 0;
		
		// 1 累加求和
		for (FlowBean flowBean : values) {
			sum_upFlow += flowBean.getUpFlow();
			sum_downFlow +=flowBean.getDownFlow();
		}
		
		FlowBean flowBean = new FlowBean(sum_upFlow,sum_downFlow);
		
		// 2 输出
		context.write(key, flowBean);
	}
}
