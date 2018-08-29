package com.weibo.hbaseInitialize;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

import static com.weibo.hbaseInitialize.TableEnum.*;

public class HBaseUtil {
	//HBase的配置对象
	private static Configuration conf = HBaseConfiguration.create ();

	public static void close(Admin admin, Connection connection) throws IOException {
		if (admin != null) {
			admin.close();
		}
		if (connection != null) {
			connection.close();
		}
	}

	public static void init() throws IOException {
		//创建微博业务命名空间
		initNamespace ();
		//创建微博内容表
		initTableContent ();
		//创建用户关系表
		initTableRelation ();
		//创建收件箱表
		initTableInbox ();
		//创建互相关注表
		initTableMutualConcern ();
	}

	//创建微博业务命名空间
	public static void initNamespace() throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();
		//创建命名空间描述器
		NamespaceDescriptor ns_weibo = NamespaceDescriptor
				.create (NS_WEIBO)
				.addConfiguration ("creator", "Adrian")
				.addConfiguration ("create_time", String.valueOf (System.currentTimeMillis ()))
				.build ();
		admin.createNamespace (ns_weibo);
		admin.close ();
		connection.close ();
	}

	/**
	 * 表名：ns_weibo:content
	 * rowKey:用户id_时间戳
	 * columnFamily：info
	 * column：content
	 * value:微博内容（文字内容，图片URL，视频URL，语音URL）
	 * versions:1
	 * ROW:  1001_1533396509519	column=info:content, value=嘿嘿嘿66
	 * ROW:  1002_1533389940665	column=info:content, value=哦，我的上帝，我要踢爆他的屁股
	 */
	public static void initTableContent() throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();

		//创建表描述器
		HTableDescriptor contentTableDescriptor = new HTableDescriptor (TableName.valueOf (TABLE_CONTENT));
		//创建列描述器
		HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor ("info");
		//设置块缓存
		infoColumnDescriptor.setBlockCacheEnabled (true);
		//设置块缓存大小 2M
		infoColumnDescriptor.setBlocksize (2 * 1024 * 1024);
		//设置版本确界
		infoColumnDescriptor.setMinVersions (1);
		infoColumnDescriptor.setMaxVersions (1);

		//将列描述器添加到表描述器中
		contentTableDescriptor.addFamily (infoColumnDescriptor);
		//创建表
		admin.createTable (contentTableDescriptor);
		admin.close ();
		connection.close ();
	}

	/**
	 * 表名：ns_weibo:relation
	 * rowKey：当前操作人的用户id
	 * columnFamily：attends，fans
	 * column：用户id
	 * value：用户id
	 * versions:1
	 * ROW:  1001	column=attends:1003, value=1003
	 * ROW:  1001	column=fans:1003, value=1003
	 */
	public static void initTableRelation() throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();
		//创建用户关系表描述器
		HTableDescriptor relationTableDescriptor = new HTableDescriptor (TableName.valueOf (TABLE_RELATION));

		//创建attends列描述器
		HColumnDescriptor attendsColumnDescriptor = new HColumnDescriptor ("attends");
		//设置块缓存
		attendsColumnDescriptor.setBlockCacheEnabled (true);
		//设置块缓存大小 2M
		attendsColumnDescriptor.setBlocksize (2 * 1024 * 1024);
		//设置版本
		attendsColumnDescriptor.setMinVersions (1);
		attendsColumnDescriptor.setMaxVersions (1);

		//创建fans列描述器
		HColumnDescriptor fansColumnDescriptor = new HColumnDescriptor ("fans");
		//设置块缓存
		fansColumnDescriptor.setBlockCacheEnabled (true);
		//设置块缓存大小 2M
		fansColumnDescriptor.setBlocksize (2 * 1024 * 1024);
		//设置版本
		fansColumnDescriptor.setMinVersions (1);
		fansColumnDescriptor.setMaxVersions (1);

		//将两个列描述器添加到表描述器中
		relationTableDescriptor.addFamily (attendsColumnDescriptor);
		relationTableDescriptor.addFamily (fansColumnDescriptor);

		//创建表
		admin.createTable (relationTableDescriptor);
		admin.close ();
		connection.close ();
	}

	/**
	 * 表名：ns_weibo:inbox
	 * rowKey：用户id
	 * columnFamily：info
	 * column：当前用户所关注的人的用户id
	 * value：微博rowkey
	 * versions:100
	 * ROW:  1001	column=info:1003, value=1003_1533389940929
	 * ROW:  1003	column=info:1001, value=1001_1533396509519
	 */
	public static void initTableInbox() throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();

		HTableDescriptor inboxTableDescriptor = new HTableDescriptor (TableName.valueOf (TABLE_INBOX));
		HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor ("info");
		//设置块缓存
		infoColumnDescriptor.setBlockCacheEnabled (true);
		//设置块缓存大小 2M
		infoColumnDescriptor.setBlocksize (2 * 1024 * 1024);
		//设置版本
		infoColumnDescriptor.setMinVersions (100);
		infoColumnDescriptor.setMaxVersions (100);

		inboxTableDescriptor.addFamily (infoColumnDescriptor);
		admin.createTable (inboxTableDescriptor);
		admin.close ();
		connection.close ();
	}

	/**
	 * 表名：ns_weibo:mutual
	 * rowKey：用户id
	 * columnFamily：mutualConcern
	 * column：当前用户所互相关注的人的用户id
	 * value：用户id
	 * versions:100
	 * ROW:  1001	column=info:1003, value=1003_1533389940929
	 * ROW:  1003	column=info:1001, value=1001_1533396509519
	 */
	public static void initTableMutualConcern() throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();

		HTableDescriptor inboxTableDescriptor = new HTableDescriptor (TableName.valueOf (TABLE_MUTUAL));
		HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor ("mutualConcern");
		//设置块缓存
		infoColumnDescriptor.setBlockCacheEnabled (true);
		//设置块缓存大小 2M
		infoColumnDescriptor.setBlocksize (2 * 1024 * 1024);
		//设置版本
		infoColumnDescriptor.setMinVersions (1);
		infoColumnDescriptor.setMaxVersions (1);

		inboxTableDescriptor.addFamily (infoColumnDescriptor);
		admin.createTable (inboxTableDescriptor);
		admin.close ();
		connection.close ();
	}

}
