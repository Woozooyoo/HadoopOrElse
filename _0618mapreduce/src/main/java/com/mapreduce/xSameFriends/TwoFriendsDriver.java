package com.mapreduce.xSameFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoFriendsDriver {

	public static void main(String[] args) throws Exception {
		args = new String[] { "F:/demo/inputFriends/outOne", 
		"F:/demo/inputFriends/outTwo3" };
		
		Configuration configuration = new Configuration();
		Job job = Job.getInstance(configuration);
		
		job.setJarByClass(TwoFriendsDriver.class);
		job.setMapperClass(TwoFriendsMapper.class);
		job.setReducerClass(TwoFriendsReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean result = job.waitForCompletion(true);
		System.exit(result?1:0);
	}
}
/*	A-B	E C 
	A-C	D F 
	A-D	E F 
	A-E	D B C 
	A-F	O B C D E 
	A-G	F E C D 
	A-H	E C D O 
	A-I	O 
	A-J	O B 
	A-K	D C 
	A-L	F E D 
	A-M	E F 
	B-C	A 
	B-D	A E 
	B-E	C 
	B-F	E A C 
	B-G	C E A 
	B-H	A E C 
	B-I	A 
	B-K	C A 
	B-L	E 
	B-M	E 
	B-O	A 
	C-D	A F 
	C-E	D 
	C-F	D A 
	C-G	D F A 
	C-H	D A 
	C-I	A 
	C-K	A D 
	C-L	D F 
	C-M	F 
	C-O	I A 
	D-E	L 
	D-F	A E 
	D-G	E A F 
	D-H	A E 
	D-I	A 
	D-K	A 
	D-L	E F 
	D-M	F E 
	D-O	A 
	E-F	D M C B 
	E-G	C D 
	E-H	C D 
	E-J	B 
	E-K	C D 
	E-L	D 
	F-G	D C A E 
	F-H	A D O E C 
	F-I	O A 
	F-J	B O 
	F-K	D C A 
	F-L	E D 
	F-M	E 
	F-O	A 
	G-H	D C E A 
	G-I	A 
	G-K	D A C 
	G-L	D F E 
	G-M	E F 
	G-O	A 
	H-I	O A 
	H-J	O 
	H-K	A C D 
	H-L	D E 
	H-M	E 
	H-O	A 
	I-J	O 
	I-K	A 
	I-O	A 
	K-L	D 
	K-O	A 
	L-M	E F 
*/