package com.atguigu.weibo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 发布微博
 * 互粉
 * 取关
 * 查看微博
 * 1、内容content表 rowKey:发布人uid_ts   cf:info     c:content   value微博内容
 * 2、用户的关注和粉丝表 rowKey:uid   cf是attend和fan c:otherUid    value:otherUid
 * 3、用户的收件箱表 rowKey:我的uid     cf:info     c:关注的人的Uid  value:otherUid_ts   ts:ts
 *
 * 互相关注维度 共同关注 可能认识的人
 * @author Adrian
 */
public class WeiBo {
	//HBase的配置对象
	private Configuration conf = HBaseConfiguration.create ();

	//创建weibo这个业务的命名空间，3张表
	private static final byte[] NS_WEIBO = Bytes.toBytes ("ns_weibo");
	private static final byte[] TABLE_CONTENT = Bytes.toBytes ("ns_weibo:content");
	private static final byte[] TABLE_RELATION = Bytes.toBytes ("ns_weibo:relation");
	private static final byte[] TABLE_INBOX = Bytes.toBytes ("ns_weibo:inbox");

	private void init() throws IOException {
		//创建微博业务命名空间
		initNamespace ();
		//创建微博内容表
		initTableContent ();
		//创建用户关系表
		initTableRelation ();
		//创建收件箱表
		initTableInbox ();
	}

	//创建微博业务命名空间
	private void initNamespace() throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);
		Admin admin = connection.getAdmin ();
		//创建命名空间描述器
		NamespaceDescriptor ns_weibo = NamespaceDescriptor
				.create ("ns_weibo")
				.addConfiguration ("creator", "JinJI")
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
	private void initTableContent() throws IOException {
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
	private void initTableRelation() throws IOException {
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
	private void initTableInbox() throws IOException {
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
	 * 发布微博
	 * a、向微博内容表中添加刚发布的内容，多了一个微博rowkey
	 * b、向发布微博人的粉丝的收件箱表中，添加该微博rowkey
	 *
	 * @param uid     发布人id
	 * @param content 发布内容
	 */
	public void publishContent(String uid, String content) throws IOException {
		Connection connection = ConnectionFactory.createConnection (conf);

		/**a 向微博内容表中添加刚发布的内容，多了一个微博rowkey*/
		//得到微博表对象
		Table contentTable = connection.getTable (TableName.valueOf (TABLE_CONTENT));
		//组装rowkey
		long ts = System.currentTimeMillis ();
		String rowkey = uid + "_" + ts;
		//添加微博内容到微博表
		Put contentPut = new Put (Bytes.toBytes (rowkey));
		contentPut.addColumn (Bytes.toBytes ("info"), Bytes.toBytes ("content"), Bytes.toBytes (content));
		contentTable.put (contentPut);

		/**b 向发布微博人的粉丝的收件箱表中，添加该微博rowkey*/
		//查询用户关系表，得到当前用户的fans用户id
		Table relationTable = connection.getTable (TableName.valueOf (TABLE_RELATION));
		//获取粉丝的用户id
		Get get = new Get (Bytes.toBytes (uid));
		get.addFamily (Bytes.toBytes ("fans"));

		//先取出所有fans的用户id，存放于一个集合之中
		List<byte[]> fansList = new ArrayList<> ();

		Result result = relationTable.get (get);
		Cell[] cells = result.rawCells ();
		for (Cell cell : cells) {
			//取出当前用户所有的粉丝uid
			fansList.add (CellUtil.cloneValue (cell));
		}

		//如果没有粉丝，则不需要操作粉丝的收件箱表
		if (fansList.size () <= 0) return;

		//开始操作收件箱表
		Table inboxTable = connection.getTable (TableName.valueOf (TABLE_INBOX));
		//封装用于操作粉丝收件箱表的Put对象集合
		List<Put> putsList = new ArrayList<> ();
		for (byte[] fansRowKey : fansList) {
			Put inboxPut = new Put (fansRowKey);
			inboxPut.addColumn (Bytes.toBytes ("info"), Bytes.toBytes (uid), ts, Bytes.toBytes (rowkey));
			putsList.add (inboxPut);
		}
		//向收件箱表放置数据
		inboxTable.put (putsList);

		//关闭表与连接器，释放资源
		inboxTable.close ();
		relationTable.close ();
		contentTable.close ();
		connection.close ();
	}

	/**
	 * 关注操作
	 * a、在用户关系表中，对当前主动操作的用户id进行添加关注的操作
	 * b、在用户关系表中，对被关注的人的用户id，添加粉丝操作
	 * c、对当前操作的用户的收件箱表中，添加他所关注的人的最近的微博rowkey
	 *
	 * @param uid     操作者id
	 * @param attends 关注的人的id数组
	 */
	public void addAttends(String uid, String... attends) throws IOException {
		//参数过滤:如果没有传递关注的人的uid，则直接返回
		if (attends == null || attends.length <= 0 || uid == null) return;

		/**a 对当前主动操作的用户id进行添加关注的操作*/
		Connection connection = ConnectionFactory.createConnection (conf);
		Table relationTable = connection.getTable (TableName.valueOf (TABLE_RELATION));

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

		/**c 对当前操作的用户的收件箱表中，添加他所关注的人的最近的微博rowKey*/
		//取得微博内容表
		Table contentTable = connection.getTable (TableName.valueOf (TABLE_CONTENT));
		Scan scan = new Scan ();
		//用于存放扫描出来的我所关注的人的微博rowKey
		List<byte[]> contentRowkeysList = new ArrayList<> ();

		for (String attend : attends) {
			//1002_152321283837374
			//扫描微博rowkey，使用rowfilter过滤器
			RowFilter filter = new RowFilter (CompareFilter.CompareOp.EQUAL, new SubstringComparator (attend + "_"));
			scan.setFilter (filter);
			//通过该scan扫描结果
			ResultScanner resultScanner = contentTable.getScanner (scan);
			Iterator<Result> iterator = resultScanner.iterator ();
			while (iterator.hasNext ()) {
				Result result = iterator.next ();
				contentRowkeysList.add (result.getRow ());
			}
		}
		//将取出的微博contentRowkey放置于当前操作的这个用户的收件箱表中
		//如果所关注的人，没有一条微博，则直接返回
		if (contentRowkeysList.size () <= 0) return;

		//操作inboxTable
		Table inboxTable = connection.getTable (TableName.valueOf (TABLE_INBOX));
		Put inboxPut = new Put (Bytes.toBytes (uid));
		for (byte[] rowkey : contentRowkeysList) {
			//拆分 contentRowkey 成 uid和微博发布时间戳
			String rowkeyString = Bytes.toString (rowkey);
			String attendUID = rowkeyString.split ("_")[0];
			String attendWeiboTS = rowkeyString.split ("_")[1];
			inboxPut.addColumn (Bytes.toBytes ("info"), Bytes.toBytes (attendUID), Long.valueOf (attendWeiboTS), rowkey);
		}
		inboxTable.put (inboxPut);

		//关闭，释放资源
		inboxTable.close ();
		contentTable.close ();
		relationTable.close ();
		connection.close ();
	}

	/**
	 * 取关操作
	 * a、在用户关系表中，删除你要取关的那个人的用户id
	 * b、在用户关系表中，删除被你取关的那个人的粉丝中的当前操作用户id
	 * c、删除微博收件箱表中你取关的人所发布的微博的rowkey
	 *
	 * @param uid     操作者id
	 * @param attends 关注的人的id数组
	 */
	public void removeAttends(String uid, String... attends) throws IOException {
		//参数过滤:如果没有传递关注的人的uid，则直接返回
		if (attends == null || attends.length <= 0 || uid == null) return;

		Connection connection = ConnectionFactory.createConnection (conf);
		/**a、在用户关系表中，删除你要取关的那个人的用户id*/
		//得到用户关系表/***/
		Table relationTable = connection.getTable (TableName.valueOf (TABLE_RELATION));
		Delete attendDelete = new Delete (Bytes.toBytes (uid));
		List<Delete> deletes = new ArrayList<> ();
		for (String attend : attends) {
			/**b 在对面用户关系表中移除粉丝*/
			attendDelete.addColumn (Bytes.toBytes ("attends"), Bytes.toBytes (attend));
			Delete delete = new Delete (Bytes.toBytes (attend));
			delete.addColumn (Bytes.toBytes ("fans"), Bytes.toBytes ("uid"));
			deletes.add (delete);
		}
		deletes.add (attendDelete);
		relationTable.delete (deletes);

		/**c、删除微博收件箱表中你取关的人所发布的微博的rowkey*/
		Table inboxTable = connection.getTable (TableName.valueOf (TABLE_INBOX));

		Delete delete = new Delete (Bytes.toBytes (uid));
		for (String attend : attends) {
			delete.addColumns (Bytes.toBytes ("info"), Bytes.toBytes (attend));
		}
		inboxTable.delete (delete);

		//释放资源
		inboxTable.close ();
		relationTable.close ();
		connection.close ();
	}

	/**
	 * 查看微博内容
	 * a、从微博收件箱中获取所有关注的人发布的微博的微博rowkey
	 * b、根据得到的微博rowkey，去微博内容表中得到数据
	 * c、将取出的数据解码然后封装到Message对象中
	 *
	 * @param uid 操作者id
	 */
	public List<Message> getAttendsContent(String uid) throws IOException {
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

	/**
	 * 测试
	 *
	 * @param
	 * @throws IOException
	 */

	//发布微博
	public static void publishWeiBoTest(WeiBo weiBo, String uid, String content) throws IOException {
		weiBo.publishContent (uid, content);
	}

	//关注
	public static void addAttendTest(WeiBo weiBo, String uid, String... attends) throws IOException {
		weiBo.addAttends (uid, attends);
	}

	//取关
	public static void removeAttendTest(WeiBo weiBo, String uid, String... attends) throws IOException {
		weiBo.removeAttends (uid, attends);
	}

	//刷微博
	public static void scanWeiBoContentTest(WeiBo weiBo, String uid) throws IOException {
		List<Message> list = weiBo.getAttendsContent (uid);
		System.out.println (list);
	}

	//看某人微博
	public static void scanSomebodyContentTest(WeiBo weiBo, String uid, String otherUid) throws IOException {
		List<Message> list = weiBo.getAttendsContent (uid);
		System.out.println (list);
	}

	public static void main(String[] args) throws IOException {
		WeiBo wb = new WeiBo ();
//		wb.init();
//
//		publishWeiBoTest(wb, "1002", "哦，我的上帝，我要踢爆他的屁股");
//		publishWeiBoTest(wb, "1002", "哦，我的上帝，我还要踢爆他的屁股");
//		publishWeiBoTest(wb, "1002", "哦，我的上帝，我非要踢爆他的屁股");
//		publishWeiBoTest(wb, "1003", "哦，我的上帝，我也要踢爆他的屁股");
//
//		addAttendTest(wb, "1001", "1002", "1003");
//		removeAttendTest(wb, "1001", "1002");
//		scanWeiBoContentTest(wb, "1001");

//		addAttendTest (wb, "1003", "1002", "1001","1004");
		scanWeiBoContentTest (wb, "1003");

//		publishWeiBoTest (wb, "1001", "嘿嘿嘿11");
//		publishWeiBoTest (wb, "1001", "嘿嘿嘿22");
//		publishWeiBoTest (wb, "1001", "嘿嘿嘿33");
//		publishWeiBoTest (wb, "1001", "嘿嘿嘿44");
//		publishWeiBoTest (wb, "1001", "嘿嘿嘿55");
//		publishWeiBoTest (wb, "1001", "嘿嘿嘿66");
//		scanWeiBoContentTest (wb, "1003");
	}
}