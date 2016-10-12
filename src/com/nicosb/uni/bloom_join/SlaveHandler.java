package com.nicosb.uni.bloom_join;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

public class SlaveHandler implements Runnable {
	private Socket slaveSocket;
	private MasterServer master;
	private Connection conn;
	
	
	public SlaveHandler(Socket slaveSocket, MasterServer master) {
		super();
		this.slaveSocket = slaveSocket;
		this.master = master;		
	}

	@Override
	public void run() {
		System.out.println("handler started for slave id=" + master.getSocketCount());
		do{
			try {
				DataInputStream input = new DataInputStream(slaveSocket.getInputStream());
	
				int slavePort = slaveSocket.getLocalPort();
				while(input.available() < 1){
					input = new DataInputStream(slaveSocket.getInputStream());
				}
				char c = input.readChar();
				System.out.println("received message from slave " + master.getSocketCount() + "(" + c + ")");
				switch(c){
					case 'r':
						String tables = input.readUTF();
						if(registerServer(slavePort, tables)){
							master.putSocket(master.getSocketCount(), slaveSocket);
							System.out.println("registered slave with port: " + slavePort);
						}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}while(slaveSocket.isConnected());
	}
	
	/**
	 * <pre> tables contains the tables of a server separated by ';'
	 * @param port the server's port, serves as an identifier in a local environment
	 * @param tables the string containing the server's table's separated by ';'
	 * 
	 * registers the server by inserting its properties into the master's register
	 */
	protected boolean registerServer(int port, String tables) {
		String[] tables_split = tables.split(";");
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/bloom_join";
			Properties props = new Properties();
			props.setProperty("user", System.getenv("DB_USER"));
			props.setProperty("password", System.getenv("DB_PASSWORD"));
			conn = DriverManager.getConnection(url, props);
			PreparedStatement prep = conn.prepareStatement("INSERT INTO sitetables(siteport, tablename) VALUES(" + master.getSocketCount() + ", ?)");

			for(String t: tables_split){
				prep.setString(1, t);
				prep.execute();
			}
			conn.close();
			return true;
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

}
