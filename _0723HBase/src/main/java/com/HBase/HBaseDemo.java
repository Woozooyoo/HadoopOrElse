package com.HBase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseDemo {

	public static Configuration conf;

	static {
		conf = HBaseConfiguration.create ();
	}

	/************** distinguish the existence of table ************/

	public static boolean isExist(String tableName) throws IOException {
		// Deprecated API
		// HBaseAdmin admin = new HBaseAdmin(conf);
		// return admin.tableExists(TableName.valueOf(tableName));

		// New API
		Admin admin2 = ConnectionFactory.createConnection (conf).getAdmin ();

		return admin2.tableExists (TableName.valueOf (tableName));
	}

	/************** create table **********************************/

	public static void createTable(String tableName, String... columnFamilies) throws IOException {

		if (isExist (tableName)) {
			System.out.println ("table exists");
			return;
		}

		Admin admin = ConnectionFactory.createConnection (conf).getAdmin ();

		HTableDescriptor htd = new HTableDescriptor (TableName.valueOf (tableName));

		if (columnFamilies.length == 0) {
			System.out.println ("Column family not set");
			return;
		}

		for (String columnFamily : columnFamilies) {
			htd.addFamily (new HColumnDescriptor (columnFamily));
		}
		// create table
		admin.createTable (htd);
		System.out.println ("table creates successfully");

	}

	/************** delete table **********************************/

	public static void deleteTable(String tableName) throws IOException {
		if (!isExist (tableName)) {
			System.out.println (tableName + "表不存在！！！");
			return;
		}

		Admin admin = ConnectionFactory.createConnection (conf).getAdmin ();

		if (!admin.isTableDisabled (TableName.valueOf (tableName))) {
			admin.disableTable (TableName.valueOf (tableName));
		}
		admin.deleteTable (TableName.valueOf (tableName));
		System.out.println ("delete successfully");

	}

	/************** add a row of data *****************************/

	public static void addRow(String tableName, String rowKey, String columnFamily, String qualifier, String value)
			throws IOException {
		// Deprecated API
		// HTable table = new HTable(conf, tableName);

		Table table = ConnectionFactory.createConnection (conf).getTable (TableName.valueOf (tableName));

		Put put = new Put (Bytes.toBytes (rowKey));

		put.addColumn (Bytes.toBytes (columnFamily), Bytes.toBytes (qualifier), Bytes.toBytes (value));
//		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("addr"), Bytes.toBytes("shenzhen"));//对同一个rowkey添加多列数据

		table.put (put);
		// table.put(List<put>);
		table.close ();
	}

	/************** delete a row of data **************************/

	public static void deleteRow(String tableName, String rowKey, String columnFamily, String column) throws IOException {

		Table table = ConnectionFactory.createConnection (conf).getTable (TableName.valueOf (tableName));

		Delete delete = new Delete (Bytes.toBytes (rowKey));
//		delete.addColumns (Bytes.toBytes (columnFamily), Bytes.toBytes (column));//删除所有版本的
//		delete.addColumn (Bytes.toBytes (columnFamily), Bytes.toBytes (column));//删除最新版本

		table.delete (delete);

		table.close ();
	}

	/************** delete multi-row of data **********************/

	public static void deleteMultiRow(String tableName, String... rowKeys) throws IOException {

		Table table = ConnectionFactory.createConnection (conf).getTable (TableName.valueOf (tableName));

		List<Delete> list = new ArrayList<> ();

		for (String rowKey : rowKeys) {
			Delete delete = new Delete (Bytes.toBytes (rowKey));
			list.add (delete);
		}

		table.delete (list);
		table.close ();
	}

	/************** get all rows *********************************/

	public static void getAllRows(String tableName) throws IOException {

		Table table = ConnectionFactory.createConnection (conf).getTable (TableName.valueOf (tableName));

		Scan scan = new Scan (); // default scan all table
		// scan.setStartRow(startRow);
		// scan.addColumn(family, qualifier);
		// scan.addFamily(family);
		scan.setMaxVersions (); //scan all versions

		ResultScanner resultScanner = table.getScanner (scan);

		// Iterator can't delete dynamically
		for (Result result : resultScanner) {
			// result.getRow(); // just the data of a rowKey
//			System.out.println (Bytes.toString (result.getRow ()));
			Cell[] cells = result.rawCells (); // a cell

			for (Cell cell : cells) {
				System.out.print ("ROW:  " + Bytes.toString (CellUtil.cloneRow (cell)) + "	");
				System.out.print ("column=" + Bytes.toString (CellUtil.cloneFamily (cell)) + ":"
						+ Bytes.toString (CellUtil.cloneQualifier (cell)));
				System.out.println (", value=" + Bytes.toString (CellUtil.cloneValue (cell)));
			}
		}
		table.close ();
	}

	/************** get one rows *********************************/

	public static void getRow(String tableName, String rowKey) throws IOException {

		Table table = ConnectionFactory.createConnection (conf).getTable (TableName.valueOf (tableName));

		Get get = new Get (Bytes.toBytes (rowKey));

		// get.addFamily(Bytes.toBytes("info1"));// set a filter columnFamily

//		get.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"));// set a filter column
//		get.setMaxVersions (2);

		Result result = table.get (get);

		System.out.println (Bytes.toString (result.getRow ()));

		Cell[] cells = result.rawCells (); // a cell

		for (Cell cell : cells) {
			System.out.print ("ROW:  " + Bytes.toString (CellUtil.cloneRow (cell)) + "	");
			System.out.print ("column=" + Bytes.toString (CellUtil.cloneFamily (cell)) + ":"
					+ Bytes.toString (CellUtil.cloneQualifier (cell)));
			System.out.println (", value=" + Bytes.toString (CellUtil.cloneValue (cell)));
		}

		table.close ();
	}

	// Test
	public static void main(String[] args) throws IOException {
//		 createTable("hbase_book", "info");
//		 deleteTable("ns_ct:calllog");
//		 addRow("staff", "1001", "info2", "name", "bob");
//		 addRow("staff", "1001", "info1", "ok", "ojbk");
//		 addRow("staff", "1003", "info1", "name", "kotlin");
//		 deleteRow("staff", "1001", null);
//		 deleteMultiRow("staff", "1001", "1002", "1003");
		getAllRows ("ns_weibo:relation");
//		 getRow("staff", "1001");
//		 System.out.println(isExist("fruit"));
	}
}
