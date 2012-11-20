package com.dp.nebula.wormhole.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.engine.monitor.WormHoleJobInfo;

public final class JobDBUtil {
	
	private static final Log s_logger = LogFactory.getLog(JobDBUtil.class);
	
	private JobDBUtil(){
	}
	
	public static String getUrl(){
		Properties props = new Properties();
		try {
			FileInputStream in = new FileInputStream(new File(Environment.JOB_INFO_DB_PROP));
			props.load(in);
			in.close();
		} catch (FileNotFoundException e) {
			s_logger.error("File not found: " + Environment.JOB_INFO_DB_PROP);
		} catch (IOException e) {
			s_logger.error("IOException while reading " + Environment.JOB_INFO_DB_PROP);
		}
		StringBuilder builder = new StringBuilder();
		String password = props.getProperty("password");
		String realPass = "";
		for(int i = 0; i < password.length(); i ++) {
			if(i%3!=0) {
				realPass += (char)(password.charAt(i)+1);
			}
		}
		builder.append("jdbc:mysql://").append(props.getProperty("ip"))
			.append(":").append(props.getProperty("port"))
			.append("/").append(props.getProperty("database"))
			.append("?user=").append(props.getProperty("user"))
			.append("&password=").append(realPass);
		return builder.toString();
	}
	
	public static void insert(String sql){
		String url = getUrl();
		Connection conn;
		Statement stmt;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(url);
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			stmt.close();
			conn.close();
		} catch (ClassNotFoundException e) {
			s_logger.error("Class Not found! " + e.getMessage());  
		} catch(SQLException e){  
			s_logger.error("SQLException: " + e.getMessage());  
		}
	}
	
	public static void insertOneJobInfo(WormHoleJobInfo value) {
		String sql = "INSERT INTO Wormhole_Job_Info( DataSource, DataTarget, ResultCode, CostTime, TotalBytes, TotalLines, UserName,StartTime) VALUES "
			+ value.getString();
		insert(sql);
	}
	
//	public static void main(String []args){
//		int l = 100000;
//		prepare(l);
//		noprepare(l);
//	}
//	public static void prepare(int l){
//		StringBuilder builder = new StringBuilder();
//		builder.append("jdbc:mysql://").append("192.168.32.102")
//			.append(":").append("3306")
//			.append("/").append("test")
//			.append("?user=").append("root")
//			.append("&password=").append("sunny");
//		String url = builder.toString();
//		Date now = new Date();
//		Connection conn;
//		PreparedStatement stmt;
//		String sql = "insert into c (id,name) values (?,?)";
//		
//		try {
//			Class.forName("com.mysql.jdbc.Driver");
//			conn = DriverManager.getConnection(url);
//			conn.setAutoCommit(false);
//
//			stmt = conn.prepareStatement(sql);
//			for(int i = 0; i < l; i++){
//				stmt.setInt(1, 1);
//				stmt.setString(2, "a");
//				stmt.addBatch();
//				if(( i + 1) % 1000 == 0 ){
//					stmt.executeBatch();
//
//				}
//			}
//			stmt.executeBatch();
//			conn.commit();
//			stmt.close();
//			conn.close();
//		} catch (ClassNotFoundException e) {
//			s_logger.error("Class Not found! " + e.getMessage());  
//		} catch(SQLException e){  
//			s_logger.error("SQLException: " + e.getMessage());  
//		}
//		System.out.println(new Date().getTime()-now.getTime());
//	}
//	public static void noprepare(int l){
//		StringBuilder builder = new StringBuilder();
//		builder.append("jdbc:mysql://").append("192.168.32.102")
//			.append(":").append("3306")
//			.append("/").append("test")
//			.append("?user=").append("root")
//			.append("&password=").append("sunny");
//		String url = builder.toString();
//		Date now = new Date();
//		Connection conn;
//		Statement stmt;
//		String insql = "insert into c (id,name) values";
//		String sql = insql;
//		try {
//			Class.forName("com.mysql.jdbc.Driver");
//			conn = DriverManager.getConnection(url);
//			stmt = conn.createStatement();
//			int size = 0;
//			for(int i = 0; i < l; i++){
//				if(size == 0)
//					sql = sql + "(1,\"a\")";
//				else
//					sql = sql + ",(1,\"a\")";
//				size ++;
//				if((i + 1)%1000 == 0) {
//					stmt.executeUpdate(sql);
//					sql = insql;
//					size = 0;
//				}
//			}
//		
//			if(size != 0)
//				stmt.executeUpdate(sql);
//			stmt.close();
//			conn.close();
//		} catch (ClassNotFoundException e) {
//			e.printStackTrace();
//			s_logger.error("Class Not found! " + e.getMessage());  
//		} catch(SQLException e){
//			e.printStackTrace();
//
//			s_logger.error("SQLException: " + e.getMessage());  
//		}
//		System.out.println(new Date().getTime()-now.getTime());
//	}
	
}
