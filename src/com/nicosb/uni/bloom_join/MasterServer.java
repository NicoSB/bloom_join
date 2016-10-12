package com.nicosb.uni.bloom_join;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Scanner;

public class MasterServer implements Server{
	public static final char CHAR_BLOOMFILTER = 'b';
	public static final char CHAR_TUPLES = 't';
	public static final char CHAR_TERMINATE = 'q';
	public static final char CHAR_REGISTER = 'q';
	
	private Connection conn;

	public MasterServer(){
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/";
			Properties props = new Properties();
			props.setProperty("user", System.getenv("DB_USER"));
			props.setProperty("password", System.getenv("DB_PASSWORD"));
			conn = DriverManager.getConnection(url, props);
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	@Override
	public void run(String hostName, int port) {
		try {
			ServerSocket masterSocket = new ServerSocket(port);
			System.out.println("master server started on port " + masterSocket.getLocalPort());
			
			// This thread listens for registrations from other servers
			new Runnable(){

				@Override
				public void run() {
					while(true){
						try {
							Socket slaveSocket = masterSocket.accept();
							DataInputStream input = new DataInputStream(slaveSocket.getInputStream());
							while(input.available() < 1){
								input = new DataInputStream(slaveSocket.getInputStream());
							}
							
							String tables = input.readUTF();
							registerServer(port, tables);
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
							
					}
				}
				
			};
			
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {	
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Scanner s = new Scanner(System.in);
		while(true){
			System.out.print("psql>>>");
			String query = s.nextLine();
			try {
				QueryInformation qi = new QueryInformation(query);
				qi.getTables();
			} catch (InvalidQueryException e) {
				System.out.println("ERROR: Invalid input!");
			}
		}
	}

	/**
	 * <pre> tables contains the tables of a server separated by ';'
	 * @param port the server's port, serves as an identifier in a local environment
	 * @param tables the string containing the server's table's separated by ';'
	 * 
	 * registers the server by inserting its properties into the master's register
	 */
	protected void registerServer(int port, String tables) {
		String[] tables_split = tables.split(";");
		try {
			PreparedStatement prep = conn.prepareStatement("INSERT INTO slave_tables(server_id, table_name) VALUES(" + port + ", ?)");

			for(String t: tables_split){
				prep.setString(1, t);
				prep.execute();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
}
