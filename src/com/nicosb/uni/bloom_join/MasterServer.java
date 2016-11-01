package com.nicosb.uni.bloom_join;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;

public class MasterServer implements Server{
	public static final char CHAR_BLOOMFILTER = 'b';
	public static final char CHAR_TUPLES = 't';
	public static final char CHAR_TERMINATE = 'q';
	public static final char CHAR_REGISTER = 'r';
	
	private Connection conn;
	private HashMap<Integer, Socket> socketMap = new HashMap<>();
	private HashMap<Integer, ObjectOutputStream> ostreamMap = new HashMap<>();
	private ResultSet siteTables;
	public QueryInformation cachedQuery;
	public BloomProcessor activeProcessor;
	public JoinProcessor joinProcessor;
	public String latestQuery;
	
	
	public MasterServer(){
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/bloom_join";
			Properties props = new Properties();
			props.setProperty("user", System.getenv("DB_USER"));
			props.setProperty("password", System.getenv("DB_PASSWORD"));
			conn = DriverManager.getConnection(url, props);
			conn.createStatement().executeUpdate("TRUNCATE TABLE sitetables");
			
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
			@SuppressWarnings("resource")
			ServerSocket masterSocket = new ServerSocket(port);
			CustomLog.println("master server started on port " + masterSocket.getLocalPort());
			CustomLog.printToFile = true;

			// This thread listens for registrations from other servers
			class ConnectionListener implements Runnable{
				MasterServer master;
				ConnectionListener(MasterServer master){ this.master = master; }
				@Override
				public void run() {
					while(true){
						Socket slaveSocket;
						try {
							slaveSocket = masterSocket.accept();
							SlaveHandler sh = new SlaveHandler(slaveSocket, master);
							new Thread(sh).start();
						} catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			};
			
			new Thread(new ConnectionListener(this)).start();;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {	
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		@SuppressWarnings("resource")
		Scanner s = new Scanner(System.in);
		while(true){
			CustomLog.print("psql>>>");
			latestQuery = s.nextLine();
			try {
				cachedQuery = new QueryInformation(latestQuery);
				siteTables = QueryEvaluator.evaluate(cachedQuery, this);
			} catch (InvalidQueryException e) {
				CustomLog.println("ERROR: Invalid input!");
			}
		}
	}

		
	/**
	 * 
	 * @param port the server's port
	 * @return the corresponding socket or null if there is none.
	 */
	public Socket getSocket(int port){
		return socketMap.get(port);
	}
	
	public void putSocket(int slavePort, Socket slaveSocket) {
		socketMap.put(slavePort, slaveSocket);
	}

	public int getSocketCount() {
		// TODO Auto-generated method stub
		return socketMap.size();
	}
	
	public void sendBloomFilter(byte[] bloomfilter){
		try {
			siteTables.beforeFirst();
			while(siteTables.next()){
				Socket slave = getSocket(siteTables.getInt(1));
				if(slave != null){
					ObjectOutputStream out = ostreamMap.get(siteTables.getInt(1));
					out.writeObject("t;k=6,m=2000");
					String attr = cachedQuery.getJoinAttributes().get(siteTables.getString(2));
					out.writeObject("SELECT DISTINCT " + attr + " FROM " + siteTables.getString(2));
					out.writeObject(bloomfilter);
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void putOStream(int socketId, ObjectOutputStream objectOutputStream) {
		ostreamMap.put(socketId, objectOutputStream);
	}

	public ObjectOutputStream getOStream(int socketId){
		return ostreamMap.get(socketId);
	}
	
	
	
	
}
