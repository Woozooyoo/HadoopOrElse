package com.atguigu.mapreduce.yCompress;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

public class TestCompress {

	public static void main(String[] args) throws Exception, IOException {
		// compress("F:/helloworld.txt","org.apache.hadoop.io.compress.BZip2Codec");
		// compress("F:/helloworld.txt","org.apache.hadoop.io.compress.GzipCodec");
		// compress("F:/helloworld.txt","org.apache.hadoop.io.compress.DefaultCodec");
		// decompres("F:/helloworld.txt.bz2");
		// decompres("F:/helloworld.txt.gz");
		decompres("F:/helloworld.txt.deflate");
	}

	/*
	 * 压缩 filername：要压缩文件的路径
	 * method：欲使用的压缩的方法（org.apache.hadoop.io.compress.BZip2Codec）
	 */
	public static void compress(String filername, String method) throws ClassNotFoundException, IOException {

		// 1 创建压缩文件路径的输入流
		File fileIn = new File(filername);
		InputStream in = new FileInputStream(fileIn);

		// 2 获取压缩的方式的类
		Class codecClass = Class.forName(method);

		Configuration conf = new Configuration();
		// 3 通过名称找到对应的编码/解码器
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

		// 4 该压缩方法对应的文件扩展名
		File fileOut = new File(filername + codec.getDefaultExtension());

		OutputStream out = new FileOutputStream(fileOut);

		CompressionOutputStream cos = codec.createOutputStream(out);

		// 5 流对接
		IOUtils.copyBytes(in, cos, 1024 * 1024 * 5, false); // 缓冲区设为5MB

		// 6 关闭资源
		in.close();
		cos.close();
		out.close();
	}

	/*
	 * 解压缩 filename：希望解压的文件路径
	 */
	public static void decompres(String filename) throws FileNotFoundException, IOException {

		CompressionCodecFactory factory = new CompressionCodecFactory(new Configuration());

		// 1 获取文件的压缩方法
		CompressionCodec codec = factory.getCodec(new Path(filename));

		// 2 判断该压缩方法是否存在
		if (null == codec) {
			System.out.println("Cannot find codec for file " + filename);
			return;
		}

		// 3 创建压缩文件的输入流
		CompressionInputStream cis = codec.createInputStream(new FileInputStream(filename));

		// 4 创建解压缩文件的输出流
		File fout = new File(filename + ".decoded");
		OutputStream out = new FileOutputStream(fout);

		// 5 流对接
		IOUtils.copyBytes(cis, out, 1024 * 1024 * 5, false);

		// 6 关闭资源
		cis.close();
		out.close();
	}
}
