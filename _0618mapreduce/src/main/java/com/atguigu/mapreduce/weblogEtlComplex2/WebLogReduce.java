package com.atguigu.mapreduce.weblogEtlComplex2;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WebLogReduce extends Reducer<WebLogBean, NullWritable, WebLogBean, NullWritable> {
	@Override
	protected void reduce(WebLogBean key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		context.write(key, NullWritable.get());
	}
}
