package com.sanfrancisco;

import java.sql.*;

public class KylinJdbc {
	public static void main(String[] args) throws Exception {
		//Kylin_JDBC 驱动
		String KYLIN_DRIVER = "org.apache.kylin.jdbc.Driver";
		//Kylin_URL
		String KYLIN_URL = "jdbc:kylin://hadoop102:7070/EmpDept";
		//Kylin的用户名
		String KYLIN_USER = "ADMIN";
		//Kylin的密码
		String KYLIN_PASSWD = "KYLIN";

		//添加驱动信息
		Class.forName(KYLIN_DRIVER);

		//获取连接
		Connection connection = DriverManager.getConnection(KYLIN_URL, KYLIN_USER, KYLIN_PASSWD);

		//预编译SQL
		PreparedStatement ps = connection.prepareStatement("SELECT deptno,sum(sal) FROM emp group by deptno");

		//执行查询
		ResultSet resultSet = ps.executeQuery();

		System.out.println("deptno\t" + "sum_sal");
		//遍历打印
		while (resultSet.next()) {
			System.out.println(resultSet.getString (1)+"\t\t"+resultSet.getInt(2));
		}
	}
}
