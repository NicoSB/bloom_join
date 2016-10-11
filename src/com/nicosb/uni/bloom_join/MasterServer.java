package com.nicosb.uni.bloom_join;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

public class MasterServer implements Server{
	public static final char CHAR_BLOOMFILTER = 'b';
	public static final char CHAR_TUPLES = 't';
	public static final char CHAR_TERMINATE = 'q';

	@Override
	public void run(String hostName, int port) {
		try {
			ServerSocket masterSocket = new ServerSocket(port);
			while(true){
				Socket slaveSocket = masterSocket.accept();
				new Thread(new SlaveHandler(slaveSocket)).start();
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {	
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
