package com.nicosb.uni.bloom_join;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedList;
import java.util.Properties;

public class SlaveServer implements Server {
	private ArrayList<String> tables;

	public SlaveServer(ArrayList<String> tables) {
		super();
		this.tables = tables;
	}

	@Override
	public void run(String masterHostName, int masterPort) {	      
		  // create connection to server
	      Socket masterSocket;
	      try{
		      masterSocket = new Socket(masterHostName, masterPort);
		      DataOutputStream output = new DataOutputStream(masterSocket.getOutputStream());
		      output.writeUTF(""+MasterServer.CHAR_REGISTER);
		     
		      System.out.println("Connected to master on port " + masterPort);
		      // register with covered tables
		      String tablesString = "";
		      for(String s: tables){
		    	  tablesString += s;
		    	  tablesString += ";";
		      }
		      output.writeUTF(tablesString);
		      DataInputStream input = new DataInputStream(masterSocket.getInputStream());
		      do{	
		    	String header = input.readUTF();
		    	char c = header.charAt(0);
		    	switch(c){
					case MasterServer.CHAR_BLOOMFILTER:
						int k = Integer.valueOf(header.substring(header.indexOf("k=")+2,header.indexOf(",")));
						// , header.indexOf(header.indexOf(","))
						int m = Integer.valueOf(header.substring(header.indexOf("m=")+2));
						System.out.println("received bloom request from master with params (m=" + m + " k=" + k + ")");
						send(output, input.readUTF(), k, m);
						output.flush();
						break;
					case MasterServer.CHAR_TUPLES:
						System.out.println("received tuple request from master");
						//send(output, input.readUTF(), false);
						break;
					case MasterServer.CHAR_TERMINATE:
						masterSocket.close();
						break;
					}		
		      }while(masterSocket.isConnected());
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	private void send(DataOutputStream output, String query, int k, int m) {
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/bloom_join";
			Properties props = new Properties();
			props.setProperty("user", System.getenv("DB_USER"));
			props.setProperty("password", System.getenv("DB_PASSWORD"));
			
			String table = query.substring(query.indexOf("FROM ") + "FROM ".length());
			
			Connection conn = DriverManager.getConnection(url, props);
			PreparedStatement prep = conn.prepareStatement(query);
			ResultSet rs = prep.executeQuery();
			LinkedList<Integer> int_ll = new LinkedList<>();
			while(rs.next()){
				int_ll.add(rs.getInt(1));
			}
			BitSet bs = Bloomer.bloom(int_ll, k, m);
			byte[] b = bs.toByteArray();
			output.writeUTF("b;t="+table);
			output.write(b);
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