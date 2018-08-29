package com.weibo.hbaseFunction;

import com.weibo.Message;
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


public class GetAttendsContent extends FunctionDao {
	private static Configuration conf = HBaseConfiguration.create ();

	/**
	 * 查看微博内容
	 * a、从微博收件箱中获取所有关注的人发布的微博的微博rowkey
	 * b、根据得到的微博rowkey，去微博内容表中得到数据
	 * c、将取出的数据解码然后封装到Message对象中
	 *
	 * @param uid 操作者id
	 */
	public static List<Message> getAttendsContent(String uid) throws IOException {
		/**a、从微博收件箱中获取所有关注的人发布的微博的微博rowkey*/
		Connection connection = ConnectionFactory.createConnection (conf);
		Table inboxTable = connection.getTable (TableName.valueOf (TABLE_INBOX));
		//从收件箱表中获取微博rowkey
		Get inboxGet = new Get (Bytes.toBytes (uid));
		inboxGet.addFamily (Bytes.toBytes ("info"));
		//每个Cell中存储了100个版本，我们只取出最新的5个版本
		inboxGet.setMaxVersions (5);

		Result inboxResult = inboxTable.get (inboxGet);
		//准备一个存放所有微博rowkey的集合
		List<byte[]> rowkeys = new ArrayList<> ();
		Cell[] inboxCells = inboxResult.rawCells ();
		//组装rowkeys集合
		for (Cell cell : inboxCells) {
			rowkeys.add (CellUtil.cloneValue (cell));
		}

		/**b、根据得到的微博rowkey，去微博内容表中得到数据*/
		//用于批量获取所有微博数据
		List<Get> contentGets = new ArrayList<> ();
		for (byte[] rowkey : rowkeys) {
			Get contentGet = new Get (rowkey);
			contentGets.add (contentGet);
		}

		/**c、将取出的数据解码然后封装到Message对象中*/
		List<Message> messages = new ArrayList<> ();

		Table contentTable = connection.getTable (TableName.valueOf (TABLE_CONTENT));
		/* 所有的结果数据 */
		Result[] contentResults = contentTable.get (contentGets);
		for (Result r : contentResults) {
			Cell[] cs = r.rawCells ();
			for (Cell c : cs) {
				//取得contentTable中的rowkey
				String rk = Bytes.toString (r.getRow ());
				//发布微博人的UID
				String publishUID = rk.split ("_")[0];
				long publishTS = Long.valueOf (rk.split ("_")[1]);

				Message msg = new Message ();
				msg.setUid (publishUID);
				msg.setTimestamp (publishTS);
				msg.setContent (Bytes.toString (CellUtil.cloneValue (c)));

				messages.add (msg);
			}
		}

		contentTable.close ();
		inboxTable.close ();
		connection.close ();
		return messages;
	}
}
