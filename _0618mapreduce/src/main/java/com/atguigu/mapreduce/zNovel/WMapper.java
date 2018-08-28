package com.atguigu.mapreduce.zNovel;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 输入的key LongWritable 行号 输入的value Text 一行内容 输出的key Text 单词 输出的value IntWritable
 * 单词的个数
 */
public class WMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	Text k = new Text();
	IntWritable v = new IntWritable(1);
	String gj = "郭";
	// 小说写一个人、几个人、一群人、或成千成万人的性格和感情。他们的性格和感情从横面的环境中反映出来，从纵面的遭遇中反映出来，从人与人之间的交往与关系中反映出来。长篇小说中似乎只有《鲁滨逊飘流记》，才只写一个人

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// 1 一行内容转换成string
		String line = value.toString();

		// 2 切割
		String[] words = line.split("。");

		// 3 循环写出到下一个阶段
		for (String word : words) {

			String[] word2 = word.split("，");
			for (String string : word2) {
				if (word.contains("郭")) {
					k.set(gj);
					context.write(k, v);
				}
			}
			
		}
		
	}
}
