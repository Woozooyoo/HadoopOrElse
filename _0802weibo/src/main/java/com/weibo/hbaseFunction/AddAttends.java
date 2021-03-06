package com.weibo.hbaseFunction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.weibo.hbaseInitialize.TableEnum.*;

public class AddAttends extends FunctionDao {
	private static Configuration conf = HBaseConfiguration.create ();

	/**
	 * 关注操作
	 * a、在用户关系表中，对我要关注的用户id进行添加关注的操作
	 * b、在用户关系表中，对被关注的人的用户id，添加粉丝操作
	 * c、对我要关注的用户id，查询是否在我的粉丝中，如果在，就在互相关注表中双向添加互相关注
	 * d、对当前操作的用户的收件箱表中，添加他所关注的人的最近的微博rowkey
	 *
	 * @param uid     操作者id
	 * @param attends 关注的人的id数组
	 */
	public static void addAttends(String uid, String... attends) throws IOException {
		//参数过滤:如果没有传递关注的人的uid，则直接返回
		if (attends == null || attends.length <= 0 || uid == null) return;

		/************** a 对当前主动操作的用户id进行添加关注的操作 *********************************/
		Connection connection = ConnectionFactory.createConnection (conf);
		Table relationTable = connection.getTable (TableName.valueOf (TABLE_RELATION));
		Table mutualTable = connection.getTable (TableName.valueOf (TABLE_MUTUAL));

		List<Put> putsList = new ArrayList<> ();
		//在微博用户关系表中，添加新关注的好友
		Put attendPut = new Put (Bytes.toBytes (uid));
		for (String attend : attends) {
			//为当前用户添加关注人，一个attendPut对象可以addColumn多个
			attendPut.addColumn (Bytes.toBytes ("attends"), Bytes.toBytes (attend), Bytes.toBytes (attend));
			/**b 对被关注的人的用户id，添加粉丝操作*/
			//被关注的人，添加粉丝（uid）
			Put fansPut = new Put (Bytes.toBytes (attend));
			fansPut.addColumn (Bytes.toBytes ("fans"), Bytes.toBytes (uid), Bytes.toBytes (uid));
			putsList.add (fansPut);//为什么在里面， 因为fansPut在for里面创建的
		}
		putsList.add (attendPut);//为什么在外面， 因为attendPut在for外面创建的
		relationTable.put (putsList);

		/************** c、对我要关注的用户id，查询是否在我的粉丝中，如果在，就在互相关注表中双向添加互相关注 *********************************/
		List<Put> putsMutualList = new ArrayList<> ();
		Put mutualPut = new Put (Bytes.toBytes (uid));
		//对我要关注的用户id，查询是否在我的粉丝中
		Get get = new Get (Bytes.toBytes (uid));
		Result resultAttend = relationTable.get (get);
		for (String attend : attends) {
			//对我要关注的用户id，查询是否在我的粉丝中
			if (resultAttend.containsColumn (Bytes.toBytes ("fans"), Bytes.toBytes (attend))) {
				//如果在，我就加入这个id到互相关注表中
				mutualPut.addColumn (Bytes.toBytes ("mutualConcern"), Bytes.toBytes (attend), Bytes.toBytes (attend));
				//这个id也把我加到互相关注表中
				Put mutualPut2 = new Put (Bytes.toBytes (attend));
				mutualPut2.addColumn (Bytes.toBytes ("mutualConcern"), Bytes.toBytes (uid), Bytes.toBytes (uid));
				putsMutualList.add (mutualPut2);//为什么在里面， 因为mutualPut2在for里面创建的
			}
		}

		if (!mutualPut.isEmpty ())
			putsMutualList.add (mutualPut);//为什么在外面， 因为mutualPut在for外面创建的

		if (putsMutualList.size () > 0)
			mutualTable.put (putsMutualList);

		/************** d 对当前操作的用户的收件箱表中，添加他所关注的人的最近的微博rowKey *********************************/

		//取得微博内容表
		Table contentTable = connection.getTable (TableName.valueOf (TABLE_CONTENT));
		Table inboxTable = connection.getTable (TableName.valueOf (TABLE_INBOX));

		Put inboxPut = new Put (Bytes.toBytes (uid));
		//用于存放扫描出来的我所关注的人的微博rowKey

		//标记inboxPut里面是否传值
		for (String attend : attends) {
			/*Scan scan = new Scan(Bytes.toBytes(attend + "_"), Bytes.toBytes(attend + "|"));
			ResultScanner results = contentTable.getScanner(scan);
			*/
			Scan scan = new Scan ();
			//1002_152321283837374
			//扫描微博rowkey，使用rowfilter过滤器
			RowFilter filter = new RowFilter (CompareFilter.CompareOp.EQUAL, new SubstringComparator (attend + "_"));
			scan.setFilter (filter);
			//通过该scan扫描结果
			ResultScanner results = contentTable.getScanner (scan);
			//当前被关注者是否发过微博
			for (Result result : results) {
				byte[] rowKey = result.getRow ();
				//  需要传系统时间为时间戳，如果不传的话，收件箱批量Put里只有一个时间版本，也不能传发布时间为时间戳（取关再关注就没有收件箱了），
				inboxPut.addColumn (Bytes.toBytes ("info"), Bytes.toBytes (attend), System.currentTimeMillis (), rowKey);
			}
		}
		//操作inboxTable
		if (!inboxPut.isEmpty ())
			inboxTable.put (inboxPut);

		//关闭，释放资源
		inboxTable.close ();
		contentTable.close ();
		relationTable.close ();
		mutualTable.close ();
		connection.close ();
	}

}
