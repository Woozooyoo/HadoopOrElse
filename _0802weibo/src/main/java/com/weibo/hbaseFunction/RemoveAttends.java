package com.weibo.hbaseFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.weibo.hbaseInitialize.TableEnum.*;

public class RemoveAttends extends FunctionDao {
	private static Configuration conf = HBaseConfiguration.create ();

	/**
	 * 取关操作
	 * a、在用户关系表中，删除你要取关的那个人的用户id
	 * b、在用户关系表中，删除被你取关的那个人的粉丝中的当前操作用户id
	 * c、双向取消互相关注
	 * d、删除微博收件箱表中你取关的人所发布的微博的rowkey
	 *
	 * @param uid     操作者id
	 * @param attends 关注的人的id数组
	 */
	public static void removeAttends(String uid, String... attends) throws IOException {
		//参数过滤:如果没有传递关注的人的uid，则直接返回
		if (attends == null || attends.length <= 0 || uid == null) return;

		Connection connection = ConnectionFactory.createConnection (conf);
		/**a、在用户关系表中，删除你要取关的那个人的用户id*/
		//得到用户关系表/***/
		Table relationTable = connection.getTable (TableName.valueOf (TABLE_RELATION));
		Delete attendDelete = new Delete (Bytes.toBytes (uid));
		ArrayList<Delete> deletes = new ArrayList<> ();
		for (String attend : attends) {
			attendDelete.addColumns (Bytes.toBytes ("attends"), Bytes.toBytes (attend));
			/**b 在对面用户关系表中移除粉丝*/
			Delete fansDelete = new Delete (Bytes.toBytes (attend));
			fansDelete.addColumns (Bytes.toBytes ("fans"), Bytes.toBytes (uid));
			deletes.add (fansDelete);
		}
		deletes.add (attendDelete);
		relationTable.delete (deletes);

		/**c、双向取消互相关注*/
		Table mutualTable = connection.getTable (TableName.valueOf (TABLE_MUTUAL));
		Delete mutualDelete = new Delete (Bytes.toBytes (uid));
		List<Delete> mutualDeletes = new ArrayList<> ();
		for (String attend : attends) {
			mutualDelete.addColumns (Bytes.toBytes ("mutualConcern"), Bytes.toBytes (attend));
			/**b 在对面用户关系表中移除粉丝*/
			Delete mutualDelete2 = new Delete (Bytes.toBytes (attend));
			mutualDelete2.addColumns (Bytes.toBytes ("mutualConcern"), Bytes.toBytes (uid));
			mutualDeletes.add (mutualDelete2);
		}
		mutualDeletes.add (mutualDelete);
		mutualTable.delete (mutualDeletes);

		/**d、删除微博收件箱表中你取关的人所发布的微博的rowkey*/
		Table inboxTable = connection.getTable (TableName.valueOf (TABLE_INBOX));

		Delete delete = new Delete (Bytes.toBytes (uid));
		for (String attend : attends) {
			delete.addColumns (Bytes.toBytes ("info"), Bytes.toBytes (attend));
		}
		inboxTable.delete (delete);

		//释放资源
		inboxTable.close ();
		relationTable.close ();
		mutualTable.close ();
		connection.close ();
	}
}
