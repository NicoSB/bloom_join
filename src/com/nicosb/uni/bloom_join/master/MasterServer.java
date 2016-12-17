package com.nicosb.uni.bloom_join.master;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.nicosb.uni.bloom_join.Assignment;
import com.nicosb.uni.bloom_join.BloomInformation;
import com.nicosb.uni.bloom_join.CustomLog;
import com.nicosb.uni.bloom_join.QueryInformation;
import com.nicosb.uni.bloom_join.exception.InvalidQueryException;


public class MasterServer{
	public static final char CHAR_BLOOMFILTER = 'b';
	public static final char CHAR_TUPLES = 't';
	public static final char CHAR_TERMINATE = 'q';
	public static final char CHAR_REGISTER = 'r';
	
	private Connection conn;
	private HashMap<Integer, Socket> socketMap = new HashMap<>();
	private HashMap<Integer, ObjectOutputStream> ostreamMap = new HashMap<>();
	private ResultSet siteTables;
	private float errorRate = 0.05f;
	private ArrayList<String> queue = new ArrayList<>();
	private Scanner s = new Scanner(System.in);
	public boolean lock = false;
	public Assignment currentAssignment;
	public String latestQuery;
	final private float[] mEvalPs = {0.05f, 0.01f, 0.005f, 0.001f, 0.0005f, 0.0001f};
	private BufferedReader mBufferedReader = null;
	
	public MasterServer(){
		try {
			Class.forName("org.postgresql.Driver");
			String url = "jdbc:postgresql://localhost/bloom_join";
			Properties props = new Properties();
			props.setProperty("user", System.getenv("DB_USER"));
			props.setProperty("password", System.getenv("DB_PASSWORD"));
			conn = DriverManager.getConnection(url, props);
			conn.createStatement().executeUpdate("TRUNCATE TABLE sitetables");
			conn.close();
			
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public void run(String hostName, int port){
		try {
			@SuppressWarnings("resource")
			ServerSocket masterSocket = new ServerSocket(port);
			CustomLog.println("master server started on port " + masterSocket.getLocalPort());

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

		while(true){
			while(lock){
				// This is a hotfix, the evaluation would not work as intended without it. 
				// I actually have no idea why
				try {
					Thread.sleep(50);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}

			try {
				if(queue == null || queue.isEmpty()){
					CustomLog.print("psql>>>", true);
					if(mBufferedReader != null){
						String line;
						if((line = mBufferedReader.readLine()) != null){
							latestQuery = line;
							System.out.println(line);
							QueryInformation qi = new QueryInformation(line.substring(0, line.indexOf("-") - 1));
							System.out.println(qi.getMaxJoinSize());
						}
						else{
							mBufferedReader = null;
							latestQuery = s.nextLine().toLowerCase();
						}
					}
					else{
						latestQuery = s.nextLine().toLowerCase();
					}
				}
				else{
					latestQuery = queue.get(0);
				}
				currentAssignment = new Assignment(currentAssignment);
				CustomLog.printToConsole=true;
				CustomLog.printToFile=false;
				latestQuery = applyOptions(latestQuery);
				QueryInformation qi = new QueryInformation(latestQuery);
				currentAssignment.setBloomInformation(new BloomInformation(qi, errorRate));
				currentAssignment.setCachedQuery(qi);
				siteTables = QueryEvaluator.evaluate(currentAssignment.getCachedQuery(), this);
			} catch (InvalidQueryException e) {
				CustomLog.println("ERROR: Invalid input!", true);
				CustomLog.println(e.getMessage());
				lock = false;
				queue = new ArrayList<>();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

		
	private String applyOptions(String query) throws InvalidQueryException {
		Pattern p = Pattern.compile(" -([ldn]|(p [0-9].[0-9]+)|(e( [a-z]+\\.sql)?))");
		Matcher m = p.matcher(query);
		while(m.find()){
			String option = m.group();
			query = m.replaceFirst("");
			char c = option.charAt(2);
			switch(c){
					// log to file
				case 'l':
					CustomLog.printToFile = true;
					lock = true;
					break;
					// disable log
				case 'd':
					CustomLog.printToConsole = false;
					lock = true;
					break;
					// no bloom
				case 'n':
					currentAssignment.setBloom(false);
					lock = true;
					break;
				case 'p':
					errorRate = Float.valueOf(option.substring(3));
					lock = true;
					break;
				case 'e':
					if(option.contains(".sql")){
						String file = option.substring(4);
						try {
							FileReader fr = new FileReader(file);
							mBufferedReader = new BufferedReader(fr);
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					latestQuery = latestQuery.substring(0, latestQuery.indexOf("-e"));
					errorRate = 1.0f;
					queue.add(latestQuery + " -d -n");
					for(int i = 0; i < mEvalPs .length; i++){
						queue.add(String.format(latestQuery + " -d -p %.10f" , mEvalPs[i]));
					}
					currentAssignment.setEvaluating(true);
					
					QueryInformation qi = new QueryInformation(latestQuery);
					queue.add(String.format(latestQuery + " -d -p %.10f" , 1.0f/(qi.getMaxJoinSize())));
					queue.add(String.format(latestQuery + " -d -p %.10f" , 1.0f/(qi.getMaxJoinSize()*1.1f)));
					queue.add(String.format(latestQuery + " -d -p %.10f" , 1.0f/(qi.getMaxJoinSize()*10)));						
					latestQuery = applyOptions(queue.get(0));
					lock = true;
					break;
			}
			m = p.matcher(query);
		}
		return query.trim();
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
	
	public void pop(){
		queue.remove(0);
	}
	
	public boolean evalFinished(){
		return queue.isEmpty();
	}
	
	public void sendBloomFilter(byte[] bloomfilter){
		try {
			siteTables.beforeFirst();
			while(siteTables.next()){
				Socket slave = getSocket(siteTables.getInt(1));
				if(slave != null){
					ObjectOutputStream out = ostreamMap.get(siteTables.getInt(1));
					out.writeObject("t;k="+currentAssignment.getBloomInformation().getHashCount()+",m="+currentAssignment.getBloomInformation().getFilterSize());
					String attr = currentAssignment.getCachedQuery().getJoinAttributes().get(siteTables.getString(2));
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

	public void sendIndices(String[] vals) throws SQLException, IOException{
		siteTables.beforeFirst();
		while(siteTables.next()){
			Socket slave = getSocket(siteTables.getInt(1));
			if(slave != null){
				ObjectOutputStream out = ostreamMap.get(siteTables.getInt(1));
				String table = siteTables.getString(2);
				String attr = currentAssignment.getCachedQuery().getJoinAttributes().get(table);
				out.writeObject("t;t="+table+"a="+attr+"c=s");
				out.writeObject(vals);
			}
		}		
	}
	
	public void sendIndices(Integer[] vals) throws SQLException, IOException{
		siteTables.beforeFirst();
		while(siteTables.next()){
			Socket slave = getSocket(siteTables.getInt(1));
			if(slave != null){
				ObjectOutputStream out = ostreamMap.get(siteTables.getInt(1));
				String table = siteTables.getString(2);
				String attr = currentAssignment.getCachedQuery().getJoinAttributes().get(table);
				out.writeObject("t;t="+table+"a="+attr+"c=i");
				out.writeObject(vals);
			}
		}		
	}
	
	public void putOStream(int socketId, ObjectOutputStream objectOutputStream) {
		ostreamMap.put(socketId, objectOutputStream);
	}

	public ObjectOutputStream getOStream(int socketId){
		return ostreamMap.get(socketId);
	}
	public boolean isBloomed(){
		return currentAssignment.isBloom();
	}

	public float getErrorRate() {
		return errorRate;
	}
	
}
