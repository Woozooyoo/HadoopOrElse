package com.weibo.hbaseFunction;

import com.weibo.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.weibo.hbaseInitialize.TableEnum.*;

public class GetSomebodyContent extends FunctionDao {
	private static Configuration conf = HBaseConfiguration.create ();

	/**
	 * 查看某人的微博内容
	 *
	 * @return
	 * @throws IOException
	 */
	public static List<Message> getSomebodyContent(String otherUid) throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Table contentTable = connection.getTable (TableName.valueOf (TABLE_CONTENT));
		Scan scan = new Scan ();
		//用于存放扫描出来的我所关注的人的微博rowKey
		List<Message> messagesList = new ArrayList<> ();

		//1002_152321283837374
		//扫描微博rowkey，使用rowfilter过滤器
		RowFilter filter = new RowFilter (CompareFilter.CompareOp.EQUAL, new SubstringComparator (otherUid + "_"));
		scan.setFilter (filter);
		//通过该scan扫描结果
		ResultScanner resultScanner = contentTable.getScanner (scan);
		for (Result r : resultScanner) {
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

				messagesList.add (msg);
			}
		}

		contentTable.close ();
		connection.close ();
		return messagesList;
	}

}
