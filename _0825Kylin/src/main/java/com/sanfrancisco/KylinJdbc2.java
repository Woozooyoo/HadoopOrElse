package com.sanfrancisco;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class KylinJdbc2 {
	public static void main(String[] args) throws Exception {

		// 加载Kylin的JDBC驱动程序
		Driver driver = (Driver) Class.forName("org.apache.kylin.jdbc.Driver").newInstance();
		// 配置登录Kylin的用户名和密码
		Properties info = new Properties();
		info.put("user", "ADMIN");
		info.put("password", "KYLIN");
		// 连接Kylin服务
		Connection conn = driver.connect("jdbc:kylin://jp01-ime-uts3-hdp39.jp01:8870/learn_kylin", info);
		//创建执行SQL的语句Statement
		Statement state = conn.createStatement();
		//处理执行结果ResultSet
		ResultSet resultSet = state.executeQuery("select part_dt, sum(price) as total_selled,count(distinct seller_id) as sellers " +
				"from kylin_sales group by part_dt order by part_dt limit 5");
		System.out.println("part_dt\t" + "\t" + "total_selled" + "\t" + "sellers");
		while (resultSet.next()) {
			String col1 = resultSet.getString(1);
			String col2 = resultSet.getString(2);
			String col3 = resultSet.getString(3);
			System.out.println(col1 + "\t" + col2 + "\t" + col3);
		}
	}
}