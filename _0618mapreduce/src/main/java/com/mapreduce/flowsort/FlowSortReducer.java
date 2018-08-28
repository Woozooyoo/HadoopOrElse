package com.mapreduce.flowsort;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
	
//	7335	110349	117684  13502468823 ----13502468823  7335	110349	117684
//	7335	110349	117684  13502468824
	@Override
	protected void reduce(FlowBean key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		context.write(values.iterator().next(), key);
	}
}
