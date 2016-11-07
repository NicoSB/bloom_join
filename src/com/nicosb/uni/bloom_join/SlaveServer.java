package com.nicosb.uni.bloom_join;

import java.io.ObjectInputStream;

import java.io.ObjectOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Properties;

import com.sun.rowset.CachedRowSetImpl;

public class SlaveServer {
	private ArrayList<String> tables;
	private HashMap<String, ResultSet> cache = new HashMap<>();
	private ObjectOutputStream output;

	public SlaveServer(ArrayList<String> tables) {
		super();
		this.tables = tables;
	}
	
	public void run(String masterHostName, int masterPort) {	      
		  // create connection to server
	      Socket masterSocket;
	      try{
		      masterSocket = new Socket(masterHostName, masterPort);
		      output = new ObjectOutputStream(masterSocket.getOutputStream());
		      output.writeObject(""+MasterServer.CHAR_REGISTER);
		     
		      CustomLog.println("Connected to master on port " + masterPort);
		      // register with covered tables
		      String tablesString = "";
		      for(String s: tables){
		    	  tablesString += s;
		    	  tablesString += ";";
		      }
		      output.writeObject(tablesString);
		      ObjectInputStream input = new ObjectInputStream(masterSocket.getInputStream());
		      do{	
		    	String header = (String)TrafficLogger.readObject(input);
		    	
		    	char c = header.charAt(0);
		    	switch(c){
					case MasterServer.CHAR_BLOOMFILTER:
						int k = Integer.valueOf(header.substring(header.indexOf("k=")+2,header.indexOf(",")));
						int m = Integer.valueOf(header.substring(header.indexOf("m=")+2));
						CustomLog.println("received bloom request from master with params (m=" + m + " k=" + k + ")");
						send(output, (String)TrafficLogger.readObject(input), k, m);
						output.flush();
						break;
					case MasterServer.CHAR_TUPLES:
						CustomLog.println("received tuple request from master");						
						int k_c = Integer.valueOf(header.substring(header.indexOf("k=")+2,header.indexOf(",")));
						int m_c = Integer.valueOf(header.substring(header.indexOf("m=")+2));
						
						String query = (String)TrafficLogger.readObject(input);
						byte[] bloomRequest = (byte[])TrafficLogger.readObject(input);
						
						ArrayList<String> results = new ArrayList<>();
						
						if(cache.containsKey(query)){
							ResultSet cachedRS = cache.get(query);
							cachedRS.beforeFirst();
							CustomLog.println(query);
							while(cachedRS.next()){
								if(Bloomer.is_in(getStrScore(cachedRS.getString(1)), BitSet.valueOf(bloomRequest), k_c, m_c)) results.add(cachedRS.getString(1));
							}
							cache.remove(query);
						}
						Class.forName("org.postgresql.Driver");
						String url = "jdbc:postgresql://localhost/bloom_join";
						Properties props = new Properties();
						props.setProperty("user", System.getenv("DB_USER"));
						props.setProperty("password", System.getenv("DB_PASSWORD"));
						
						Connection conn = DriverManager.getConnection(url, props);

						String table = query.substring(query.indexOf("FROM ") + "FROM ".length());
						String attr = query.substring(query.indexOf("DISTINCT ") + "DISTINCT ".length(), query.indexOf("FROM"));
						String select_query = "SELECT * FROM " + table + " WHERE " + attr + " IN(";
						
						for(int i = 0; i < results.size(); i++){
							select_query += "?,";
						}						

						select_query = select_query.substring(0, select_query.length() - 1);
						select_query += ")";
						
						PreparedStatement prep = conn.prepareStatement(select_query);
						prep = conn.prepareStatement(select_query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
						for(int i = 1; i  <= results.size(); i++){
							prep.setString(i, results.get(i-1));
						}			
						
						ResultSet rs = prep.executeQuery();
						CachedRowSetImpl cr = new CachedRowSetImpl();	
						cr.populate(rs);
						
						output.writeObject(MasterServer.CHAR_TUPLES+";t="+table);
						output.writeObject(cr);
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

	private void send(ObjectOutputStream output, String query, int k, int m) {
		try {
			CustomLog.println("executing " + query);
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/bloom_join";
			Properties props = new Properties();
			props.setProperty("user", System.getenv("DB_USER"));
			props.setProperty("password", System.getenv("DB_PASSWORD"));
			
			String table = query.substring(query.indexOf("FROM ") + "FROM ".length());
			
			Connection conn = DriverManager.getConnection(url, props);
			PreparedStatement prep = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
	
			ResultSet cachedRS = prep.executeQuery();
			cache.put(query, cachedRS);
			
			LinkedList<Integer> int_ll = new LinkedList<>();
			while(cachedRS.next()){
				int_ll.add(getStrScore(cachedRS.getString(1)));
			}
			BitSet bs = Bloomer.bloom(int_ll, k, m);
			byte[] b = bs.toByteArray();
			output.writeObject("b;t="+table);
			output.writeObject(b);
			conn.close();
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

	private Integer getStrScore(String string) {
		int score = 0;
		for(int i = 0; i < string.length(); i++){
			score += string.charAt(i);
		}
		return score;
	}
}	