package com.atguigu.HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseDemo_exercise {
	public static Configuration conf;

	static {
		conf = HBaseConfiguration.create ();
	}

	private static boolean isExist(String t) throws IOException {
		Admin admin = ConnectionFactory.createConnection (conf).getAdmin ();
		return admin.tableExists (TableName.valueOf (t));
	}
}
