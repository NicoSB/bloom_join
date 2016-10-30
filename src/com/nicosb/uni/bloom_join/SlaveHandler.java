package com.nicosb.uni.bloom_join;

import java.io.ObjectInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.Socket;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.BitSet;
import java.util.Properties;

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
		System.out.println("handler started for slave id=" + id);
		do{
			try {
				ObjectInputStream input = new ObjectInputStream(slaveSocket.getInputStream());
	
				int slavePort = slaveSocket.getLocalPort();
				while(input.available() < 1){
					input = new ObjectInputStream(slaveSocket.getInputStream());
				}
				String header = input.readUTF();
				char c = header.charAt(0);
				System.out.println("received message from slave " + id + "(" + c + ")");
				switch(c){
					case MasterServer.CHAR_REGISTER:
						String tables = input.readUTF();
						if(registerServer(slavePort, tables)){
							master.putSocket(master.getSocketCount(), slaveSocket);
							System.out.println("registered slave #" + id);
						}
						break;
					case MasterServer.CHAR_BLOOMFILTER:
						byte b[] = new byte[1000];
						int l = input.read(b);
						String table = header.substring(header.indexOf("t=")+"t=".length());
						System.out.println("received bloom filter from " + id + " for table " + table);
						System.out.println(b.length);
						for(int i = 0; i < l; i++){
							System.out.print(String.format("%8s", Integer.toBinaryString(b[i] & 0xFF)).replace(' ', '0'));
						}
						System.out.print("\n");

						BitSet result;
						if((result = master.activeProcessor.ORJoin(table, id, b)) != null){
							System.out.println("Joined all bloom filters: ");	
							byte[] byteArray = result.toByteArray();
							for(int i = 0; i < byteArray.length; i++){
								System.out.print(String.format("%8s", Integer.toBinaryString(byteArray[i] & 0xFF)).replace(' ', '0'));
							}
							master.sendBloomFilter(result.toByteArray());
						}
					case MasterServer.CHAR_TUPLES:
						try {
							CachedRowSetImpl tuples = new CachedRowSetImpl();
							tuples = (CachedRowSetImpl)input.readObject();
							input.close();
							tuples.beforeFirst();
							while(tuples.next()){
								System.out.print(tuples.getString(0) + tuples.getString(1));
							}
						} catch (SQLException | ClassNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						break;
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
