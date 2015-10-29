package db;

import java.sql.Connection;
import java.sql.DriverManager;

public class DataBaseJdbc {

	public static String  driver="com.mysql.jdbc.Driver";
	//online
//	public static String  url="jdbc:mysql://10.112.8.86:3306/ssyx";
//	public static String  user="ssyx";
//	public static String  password="ssyx";
	//test
	public static String  url="jdbc:mysql://127.0.0.1:3306/ssyx";
	public static String  user="wj";
	public static String  password="123";
    //online
//	public static String  driver134="com.ibm.db2.jcc.DB2Driver";
//	public static String  url134="jdbc:db2://10.112.1.134:50000/sccrm55";
//	public static String  user134="aiapp";
//	public static String  password134="as1a1nf0";
	//test
	public static String  driver134="com.ibm.db2.jcc.DB2Driver";
	public static String  url134="jdbc:db2://10.108.226.71:50000/aiapp";
	public static String  user134="aiapp";
	public static String  password134="zyx123";

	public static  Connection get85MysqlConnection(){
		System.out.println("获取 85 mq配置数据库 连接");
		Connection conn=null;
		try {
			Class.forName(driver).newInstance();
			conn = DriverManager.getConnection(url,user,password);
		} catch (Exception e) {
			System.out.println("连接 85mysql数据库出错="+e.getMessage());
		}
		return conn;
	}

	public static  Connection get134MysqlConnection(){
		System.out.println("获取 134 mq配置数据库 连接");
		Connection conn=null;
		try {
			Class.forName(driver134).newInstance();
			conn = DriverManager.getConnection(url134,user134,password134);
		} catch (Exception e) {
			System.out.println("连接134mysql数据库出错="+e.getMessage());
		}
		return conn;
	}

}
