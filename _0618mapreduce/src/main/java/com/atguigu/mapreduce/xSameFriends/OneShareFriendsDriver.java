package com.atguigu.mapreduce.xSameFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
/*	A:B,C,D,F,E,O
	B:A,C,E,K
	C:F,A,D,I
	D:A,E,F,L
	E:B,C,D,M,L
	F:A,B,C,D,E,O,M
	G:A,C,D,E,F
	H:A,C,D,E,O
	I:A,O
	J:B,O
	K:A,C,D
	L:D,E,F
	M:E,F,G
	O:A,H,I,J
	
	A	I,K,C,B,G,F,H,O,D,
	B	A,F,J,E,
	C	A,E,B,H,F,G,K,
	D	G,C,K,A,L,F,E,H,
	E	G,M,L,H,A,F,B,D,
	F	L,M,D,C,G,A,
	G	M,
	H	O,
	I	O,C,
	J	O,
	K	B,
	L	D,E,
	M	E,F,
	O	A,H,I,J,F,

	*/
public class OneShareFriendsDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "D:/Soft/DevSoft/code/demo/inputxSameFriends/in", 
		"D:/Soft/DevSoft/code/demo/inputxSameFriends/outNo2" };
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(OneShareFriendsDriver.class);
		job.setMapperClass(OneShareFriendsMapper.class);
		job.setReducerClass(OneShareFriendsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		/**********要将自定义的OutputFormat设置到job中**********/
		job.setOutputFormatClass(OneOutputformat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result?1:0);
	}
}