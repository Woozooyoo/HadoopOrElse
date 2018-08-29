package com.weibo.hbaseInitialize;

import org.apache.hadoop.hbase.util.Bytes;

public class TableEnum {
	//创建weibo这个业务的命名空间，3张表
	public static final String NS_WEIBO = "ns_weibo";
	public static final byte[] TABLE_RELATION = Bytes.toBytes ("ns_weibo:relation");
	public static final byte[] TABLE_INBOX = Bytes.toBytes ("ns_weibo:inbox");
	public static final byte[] TABLE_MUTUAL = Bytes.toBytes ("ns_weibo:mutual");
	public static final byte[] TABLE_CONTENT = Bytes.toBytes ("ns_weibo:content");
}
