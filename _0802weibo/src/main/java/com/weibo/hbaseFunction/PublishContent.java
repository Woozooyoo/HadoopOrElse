package com.weibo.hbaseFunction;

import com.weibo.hbaseInitialize.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.weibo.hbaseInitialize.TableEnum.*;

public class PublishContent extends FunctionDao {
	//HBase的配置对象
	private static Configuration conf = HBaseConfiguration.create ();
	/**
	 * 发布微博
	 * a、向微博内容表中添加刚发布的内容，多了一个微博rowkey
	 * b、向发布微博人的粉丝的收件箱表中，添加该微博rowkey
	 *
	 * @param uid     发布人id
	 * @param content 发布内容
	 */
	public static void publishContent(String uid, String content) throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);

		/**a 向微博内容表中添加刚发布的内容，多了一个微博rowkey*/
		//得到微博表对象
		Table contentTable = connection.getTable (TableName.valueOf (TABLE_CONTENT));
		//组装rowkey
		long ts = System.currentTimeMillis ();
		//添加微博内容到微博表
		Put contentPut = new Put (Bytes.toBytes (uid + "_" + ts));
		contentPut.addColumn (Bytes.toBytes ("info"), Bytes.toBytes ("content"), Bytes.toBytes (content));
		contentTable.put (contentPut);

		/**b 向发布微博人的粉丝的收件箱表中，添加该微博rowkey*/
		//查询用户关系表，得到当前用户的fans用户id
		Table relationTable = connection.getTable (TableName.valueOf (TABLE_RELATION));
		//获取粉丝的用户id
		Get get = new Get (Bytes.toBytes (uid));
		get.addFamily (Bytes.toBytes ("fans"));
		Result result = relationTable.get (get);
		//如果没有粉丝，则不需要操作粉丝的收件箱表
		if (result.isEmpty()) {
			relationTable.close();
			contentTable.close();
			HBaseUtil.close(null, connection);
			return;
		}

		//先取出所有fans的用户id,用用户id当RowKey new一个Put，存放于一个集合之中
		List<Put> putsList = new ArrayList<> ();

		for (Cell cell : result.rawCells ()) {
			//取出当前用户所有的粉丝uid
			byte[] fansRowKey =CellUtil.cloneValue (cell);
			//在粉丝的关注收件箱中添加这个微博内容的RowKey
			Put inboxPut = new Put (fansRowKey);
			inboxPut.addColumn (Bytes.toBytes ("info"), Bytes.toBytes (uid), ts, Bytes.toBytes (uid + "_" + ts));
			putsList.add (inboxPut);
		}

		//开始操作收件箱表
		Table inboxTable = connection.getTable (TableName.valueOf (TABLE_INBOX));
		//向收件箱表放置数据
		inboxTable.put (putsList);

		//关闭表与连接器，释放资源
		inboxTable.close ();
		relationTable.close ();
		contentTable.close ();
		connection.close ();
	}

}
