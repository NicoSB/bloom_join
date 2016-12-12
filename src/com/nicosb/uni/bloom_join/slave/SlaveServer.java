package com.nicosb.uni.bloom_join.slave;

import java.io.ObjectInputStream;

import java.io.ObjectOutputStream;
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

import com.nicosb.uni.bloom_join.CustomLog;
import com.nicosb.uni.bloom_join.TrafficLogger;
import com.nicosb.uni.bloom_join.master.MasterServer;
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

		      Connection conn = establishDBConnection();	
		      // register with covered tables
		      String tablesString = "";
		      for(String s: tables){
		    	  tablesString += s;
		    	  tablesString += "<";
		    	  tablesString += getCount(conn, s);
		    	  tablesString += ";";
		      }
		      
		      conn.close();
		      
		      
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
						if(header.contains("k=")){
							queryBloomed(input, header);
						}
						else if(header.contains("a=")){
							queryNotBloomed(input, header);
						}
						else{			
							String query = TrafficLogger.readObject(input);
							executeQuery(query);
						}
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

	private int getCount(Connection conn, String table) throws SQLException, ClassNotFoundException {
		String query = "SELECT COUNT(*) FROM " + table;
		ResultSet rs = conn.createStatement().executeQuery(query);
		rs.next();
		return rs.getInt(1);
	}

	private void queryNotBloomed(ObjectInputStream input, String header) {
		try {
			Integer[] vals = (Integer[])TrafficLogger.readObject(input);
			String table = header.substring(header.indexOf("t=")+"t=".length(), header.indexOf("a="));
			String attr = header.substring(header.indexOf("a=")+"a=".length());
			String query = "SELECT * FROM " + table + " WHERE " + attr + " IN(";
			Connection conn = establishDBConnection();	
			
			for(int i = 0; i < vals.length; i++){
				query += "?,";
			}						

			query = query.substring(0, query.length() - 1);
			query += ")";
			
			PreparedStatement prep = conn.prepareStatement(query);
			prep = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
			for(int i = 1; i  <= vals.length; i++){
				prep.setInt(i, Integer.valueOf(vals[i-1]));
			}	

			ResultSet rs = prep.executeQuery();
			CachedRowSetImpl cr = new CachedRowSetImpl();	
			cr.populate(rs);

			output.writeObject(MasterServer.CHAR_TUPLES+";t="+table);
			output.writeObject(cr);
			
			} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
		}
	}

	private void queryBloomed(ObjectInputStream input, String header)
			throws SQLException, ClassNotFoundException, IOException {
		int k_c = Integer.valueOf(header.substring(header.indexOf("k=")+2,header.indexOf(",")));
		int m_c = Integer.valueOf(header.substring(header.indexOf("m=")+2));
		
		String query = (String)TrafficLogger.readObject(input);
		byte[] bloomRequest = (byte[])TrafficLogger.readObject(input);
		
		ArrayList<Integer> results = new ArrayList<>();
		
		if(cache.containsKey(query)){
			ResultSet cachedRS = cache.get(query);
			cachedRS.beforeFirst();
			CustomLog.println(query);
			while(cachedRS.next()){
				if(Bloomer.is_in(cachedRS.getInt(1), BitSet.valueOf(bloomRequest), k_c, m_c)) results.add(cachedRS.getInt(1));
			}
			cache.remove(query);
		}
		else{
			// TODO fetch results when not cached
		}
		Connection conn = establishDBConnection();

		String table = query.substring(query.indexOf("FROM ") + "FROM ".length());
		String attr = query.substring(query.indexOf("DISTINCT ") + "DISTINCT ".length(), query.indexOf("FROM"));
		String select_query = "SELECT * FROM " + table + " WHERE " + attr + "IN(";
		
		for(int i = 0; i < results.size(); i++){
			select_query += "?,";
		}						

		select_query = select_query.substring(0, select_query.length() - 1);
		select_query += ")";
		
		PreparedStatement prep = conn.prepareStatement(select_query);
		prep = conn.prepareStatement(select_query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
		for(int i = 1; i  <= results.size(); i++){
			//prep.setString(i, results.get(i-1));
			prep.setInt(i, results.get(i-1));
		}			
		
		ResultSet rs = prep.executeQuery();
		CachedRowSetImpl cr = new CachedRowSetImpl();	
		cr.populate(rs);
		conn.close();
		
		output.writeObject(MasterServer.CHAR_TUPLES+";t="+table);
		output.writeObject(cr);
	}

	private Connection establishDBConnection() throws ClassNotFoundException, SQLException {
		Class.forName("org.postgresql.Driver");
		String url = "jdbc:postgresql://localhost/bloom_join";
		Properties props = new Properties();
		props.setProperty("user", System.getenv("DB_USER"));
		props.setProperty("password", System.getenv("DB_PASSWORD"));
		
		Connection conn = DriverManager.getConnection(url, props);
		return conn;
	}

	private void send(ObjectOutputStream output, String query, int k, int m) {
		try {
			String table = query.substring(query.indexOf("FROM ") + "FROM ".length());
			
			Connection conn = establishDBConnection();
			PreparedStatement prep = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
	
			ResultSet cachedRS = prep.executeQuery();
			cache.put(query, cachedRS);
			
			LinkedList<Integer> int_ll = new LinkedList<>();
			while(cachedRS.next()){
				//int_ll.add(getStrScore(cachedRS.getString(1)));
				int_ll.add(cachedRS.getInt(1));
			}
			BitSet bs = Bloomer.bloom_int(int_ll, k, m);
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
	
	private void executeQuery(String query) throws ClassNotFoundException, SQLException, IOException{
		Connection conn = establishDBConnection();	

		PreparedStatement prep = conn.prepareStatement(query);
		ResultSet rs = conn.createStatement().executeQuery(query);
		prep = conn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);		
		String table = prep.getMetaData().getTableName(1);
		CachedRowSetImpl cr = new CachedRowSetImpl();	
		cr.populate(rs);

		output.writeObject(MasterServer.CHAR_TUPLES+";t="+table);
		output.writeObject(cr);
	}
}	