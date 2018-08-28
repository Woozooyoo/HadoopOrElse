package com.HBase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestHBase {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.1.102");
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //关闭资源
    private static void close(Admin admin, Connection connection) {

        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //判断表是否存在
    private static Boolean isTableExist(String tableName) throws IOException {

        //获取配置文件
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum", "192.168.1.102");

        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();

        //获取admin对象
//        HBaseAdmin admin = new HBaseAdmin(configuration);
//        boolean exists = admin.tableExists(tableName);

        boolean exists = admin.tableExists(TableName.valueOf(tableName));

        //关闭资源
        admin.close();
        connection.close();
        return exists;
    }

    //创建表
    private static void createTable(String tableName, String... cfs) throws IOException {

        if (isTableExist(tableName)) {
            System.out.println(tableName + "表已存在！！！");
            return;
        }
        //创建一个表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        //添加列族
        if (cfs.length == 0) {
            System.out.println("未设置列族！！！");
            return;
        }

        //创建列族描述器
        for (String cf : cfs) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }

        //创建表
        admin.createTable(hTableDescriptor);

    }

    //删除表
    private static void dropTable(String tableName) throws IOException {

        if (!isTableExist(tableName)) {
            System.out.println(tableName + "表不存在！！！");
            return;
        }
        //使表不可用
        admin.disableTable(TableName.valueOf(tableName));
        //删除
        admin.deleteTable(TableName.valueOf(tableName));
    }

    //增
    private static void putData(String tableName, String rowkey, String cf, String cn, String value) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建一个Put对象
        Put put = new Put(Bytes.toBytes(rowkey));

        //添加数据
        put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn), Bytes.toBytes(value));
        table.put(put);

        //关闭资源
        table.close();
    }

    //删
    private static void deleteData(String tableName, String rowkey, String cf, String cn) throws IOException {

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建Delete对象
        Delete delete = new Delete(Bytes.toBytes(rowkey));

        delete.addColumns(Bytes.toBytes(cf), Bytes.toBytes(cn));
//        delete.addColumn()

        table.delete(delete);

        //关闭资源
        table.close();
    }

    //查（全表扫描）
    private static void scanData(String tableName) throws IOException {

        //获取表连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建一个Scan对象
        Scan scan = new Scan(Bytes.toBytes("1001"), Bytes.toBytes("1003"));

        ResultScanner results = table.getScanner(scan);

        //遍历结果集并打印
        for (Result result : results) {
            System.out.println(Bytes.toString(result.getRow()));
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                        ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                        ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }

        //关闭资源
        table.close();
    }

    //查（单条数据）
    private static void getData(String tableName, String rowkey, String cf, String cn) throws IOException {

        //获取table连接
        Table table = connection.getTable(TableName.valueOf(tableName));

        //创建一个Get对象
        Get get = new Get(Bytes.toBytes(rowkey));
//        get.setMaxVersions();

        get.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cn));

        Result result = table.get(get);
        System.out.println(Bytes.toString(result.getRow()));
        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println("CF:" + Bytes.toString(CellUtil.cloneFamily(cell)) +
                    ",CN:" + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                    ",Value:" + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        //关闭资源
        table.close();

    }

    public static void main(String[] args) throws IOException, InterruptedException {

        //判断表是否存在
//        System.out.println(isTableExist("staff"));
//        //创建一个表
//        createTable("staff", "f1");
//        System.out.println(isTableExist("staff"));
//        dropTable("staff");
//        System.out.println(isTableExist("staff"));

//        putData("staff", "1004", "f1", "addr", "shenzhen");
//        Thread.sleep(500);
//        putData("staff", "1001", "f1", "name", "zhangsan");
//        putData("staff", "1001", "f1", "sex", "male");
//        putData("staff", "1002", "f1", "sex", "female");

//        deleteData("staff", "1004", "f1", "addr");

//        scanData("staff");

        getData("student", "1001", "info", "name");

        close(admin, connection);
    }

}
