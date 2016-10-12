package com.nicosb.uni.bloom_join;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

public class QueryEvaluator {

	public static void evaluate(QueryInformation qi, MasterServer master){
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/bloom_join";
			Properties props = new Properties();
			props.setProperty("user", System.getenv("DB_USER"));
			props.setProperty("password", System.getenv("DB_PASSWORD"));
			
			Connection conn = DriverManager.getConnection(url, props);
			
			PreparedStatement prep;
			String query = "SELECT * FROM sitetables WHERE tablename IN (";
			for(int i = 0; i < qi.getTables().length; i++){
				query += "?,";
			}
			query = query.substring(0, query.length() - 1);
			query += ")";
			
			prep = conn.prepareStatement(query);
			for(int i = 1; i  <= qi.getTables().length; i++){
				prep.setString(i, qi.getTables()[i-1]);
			}
			
			ResultSet rs = prep.executeQuery();
			while(rs.next()){
				Socket slave = master.getSocket(rs.getInt(1));
				if(slave != null){
					DataOutputStream out = new DataOutputStream(slave.getOutputStream());
					out.writeUTF("b;k=6,m=66");
					out.writeUTF("SELECT DISTINCT id FROM " + rs.getString(2));
				}
			}
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
}
