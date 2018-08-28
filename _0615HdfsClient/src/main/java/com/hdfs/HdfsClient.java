package com.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Test;

public class HdfsClient {

	// 上传文件
	public static void main(String[] args) throws Exception {

		// 1 获取文件系统
		Configuration configuration = new Configuration();
		
		// (1)
		// configuration.set("fs.defaultFS", "hdfs://hadoop102:9000");
		// FileSystem fs = FileSystem.get(configuration);
		
		// (2)
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 上传文件
		fs.copyFromLocalFile(new Path("f:/hello.txt"), new Path("/hello1.txt"));

		// 3 关闭资源
		fs.close();

		System.out.println("over");
	}

	// 1 获取文件系统
	@Test
	public void initHDFS() throws Exception {

		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new Configuration());
//		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");

		// 2 打印文件系统到控制台
		System.out.println(fs.toString());

	}

	// 上传文件
	@Test
	public void copyFromLocalFileToHDFS() throws Exception {
		
		// 1 获取文件系统
		Configuration configuration = new Configuration();
		
		configuration.set("dfs.replication", "2");
		
		// (2)
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "atguigu");

		// 2 上传文件
		fs.copyFromLocalFile(new Path("f:/hello.txt"), new Path("/hello1.txt"));

		// 3 关闭资源
		fs.close();

		System.out.println("over");
	}

	// 3 下载文件
	@Test
	public void copyToLocalFileHDFS() throws Exception {

		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");

		// 2 执行下载操作  false 不删除源数据
		fs.copyToLocalFile(false, new Path("/user/atguigu/sqoop/"), new Path("D:/hello1.txt"), true);

		// 3 关闭资源
		fs.close();
	}

	// 4 创建目录
	@Test
	public void mkdirsHDFS() throws Exception {

		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");

		// 2 创建目录
		fs.mkdirs(new Path("/user/atguigu/0616"));

		// 3 关闭资源
		fs.close();
	}

	// 5 删除文件
	@Test
	public void deleteHDFS() throws Exception {
		
		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");

		// 2 执行删除
		fs.delete(new Path("/user/atguigu/0616"), true);

		// 3 关闭资源
		fs.close();
		System.out.println("over");
	}

	// 6 修改文件名称
	@Test
	public void renameHDFS() throws Exception{
		
		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");

		// 2 修改文件名称
		fs.rename(new Path("/hello.txt"),new Path("/hello6.txt"));
		
		// 3 关闭资源
		fs.close();
		System.out.println("over");
	}
	
	// 7 获取文件详情
	@Test
	public void listFilesHDFS() throws Exception{
		
		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");

		// 2 获取文件详情
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/user/hive/warehouse/staff_hive"), true);
		
		while(listFiles.hasNext()){
			LocatedFileStatus status = listFiles.next();
			
			// 输出详情
			// 文件名称
			System.out.println(status.getPath().getName());
			// 长度
			System.out.println(status.getLen());
			// 权限
			System.out.println(status.getPermission());
			// 组
			System.out.println(status.getGroup());
			
			BlockLocation[] blockLocations = status.getBlockLocations();
			
			for (BlockLocation blockLocation : blockLocations) {
				
				String[] hosts = blockLocation.getHosts();
				
				for (String host : hosts) {
					System.out.println("host "+host);
				}
			}
			
			System.out.println("--------------丽的分割线----------------");
		}
	}
	
	// 8 判断是文件还是文件夹
	@Test
	public void listStatusHDFS() throws Exception{
		
		// 1 获取文件系统
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), new Configuration(), "atguigu");

		// 2 判断是否是文件还是文件夹
		FileStatus[] listStatus = fs.listStatus(new Path("/user/hive/warehouse/staff_hive"));
		
		for (FileStatus fileStatus : listStatus) {
			// 如果是文件
			if (fileStatus.isFile()) {
				System.out.println("f:"+fileStatus.getPath().getName());
			}else {
				System.out.println("d:"+fileStatus.getPath().getName());
			}
		}
	}
}
