package com.atguigu.youtube_syllabus.etl;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.atguigu.youtube_syllabus.utils.ETLUtil;

public class VideoETLMapper extends Mapper<Object, Text, NullWritable, Text> {
	private Text v = new Text ();

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String etlString = ETLUtil.getETLString (value.toString ());

		if (StringUtils.isBlank (etlString)) return;

		v.set (etlString);

		context.write (NullWritable.get (), v);

	}

}
