package com.nicosb.uni.bloom_join.master;

import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.Properties;

import com.nicosb.uni.bloom_join.CustomLog;
import com.nicosb.uni.bloom_join.TrafficLogger;
import com.sun.rowset.CachedRowSetImpl;

public class SlaveHandler implements Runnable {
	private Socket slaveSocket;
	private MasterServer master;
	private Connection conn;
	private int id;
	
	
	public SlaveHandler(Socket slaveSocket, MasterServer master) {
		super();
		this.slaveSocket = slaveSocket;
		this.master = master;
		id = master.getSocketCount();
	}

	@Override
	public void run() {
		CustomLog.println("handler started for slave id=" + id);

		try {
			ObjectInputStream input = new ObjectInputStream(slaveSocket.getInputStream());
			do{
				int slavePort = slaveSocket.getLocalPort();
				String header = (String)TrafficLogger.readObject(input);
				char c = header.charAt(0);
				CustomLog.println("received message from slave " + id + "(" + c + ")");
				switch(c){
					case MasterServer.CHAR_REGISTER:
						String tables = (String)TrafficLogger.readObject(input);
						if(registerServer(slavePort, tables)){
							master.putSocket(master.getSocketCount(), slaveSocket);
							master.putOStream(id, new ObjectOutputStream(slaveSocket.getOutputStream()));
							CustomLog.println("registered slave #" + id);
						}
						break;
					case MasterServer.CHAR_BLOOMFILTER:
						byte b[] =  (byte[])TrafficLogger.readObject(input);
						String table = header.substring(header.indexOf("t=")+"t=".length());
						CustomLog.println("received bloom filter from " + id + " for table " + table);
						for(int i = 0; i < b.length; i++){
							CustomLog.print(String.format("%8s", Integer.toBinaryString(b[i] & 0xFF)).replace(' ', '0'));
						}
						CustomLog.println("");

						BitSet result;
						if((result = master.currentAssignment.getActiveProcessor().ORJoin(table, id, b)) != null){
							CustomLog.println("Joined all bloom filters: ");	
							byte[] byteArray = result.toByteArray();
							for(int i = 0; i < byteArray.length; i++){
								CustomLog.print(String.format("%8s", Integer.toBinaryString(byteArray[i] & 0xFF)).replace(' ', '0'));
							}
							CustomLog.println("");
							master.sendBloomFilter(result.toByteArray());
						}
						break;
					case MasterServer.CHAR_TUPLES:
						if(master.isBloomed()){
							String table_tup = header.substring(header.indexOf("t=")+"t=".length());
							CachedRowSetImpl tuples = (CachedRowSetImpl)TrafficLogger.readObject(input);
							
							tuples.beforeFirst();
							master.currentAssignment.getJoinProcessor().addRowSet(table_tup, id, tuples);
						}
						else{
							String table_tup = header.substring(header.indexOf("t=")+"t=".length());
							CachedRowSetImpl tuples = (CachedRowSetImpl)TrafficLogger.readObject(input);
							
							tuples.beforeFirst();
							master.currentAssignment.getSemiJoinProcessor().addRowSet(table_tup, id, tuples);
						}
						break;
				}
			}while(slaveSocket.isConnected());
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	/**
	 * <pre> tables contains the tables of a server separated by ';'
	 * @param port the server's port, serves as an identifier in a local environment
	 * @param tables the string containing the server's table's separated by ';'
	 * 			as well as tablename and count separated by '<'
	 * 
	 * example: table1<1337;table2<420
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
			PreparedStatement prep = conn.prepareStatement("INSERT INTO sitetables(siteport, tablename, count) VALUES(" + master.getSocketCount() + ", ?,?)");

			for(String t: tables_split){
				String[] split = t.split("<");
				prep.setString(1, split[0].toLowerCase());
				prep.setInt(2, Integer.valueOf(split[1]));
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
