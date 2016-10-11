package com.nicosb.uni.bloom_join;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;

public class SlaveHandler implements Server {
	private ArrayList<String> tables;

	public SlaveHandler(ArrayList<String> tables) {
		super();
		this.tables = tables;
	}

	@Override
	public void run(String hostName, int port) {	      
		  // create connection to server
	      Socket masterSocket;
	      try{
		      masterSocket = new Socket(hostName, port);
		      DataOutputStream output = new DataOutputStream(masterSocket.getOutputStream());
		      output.writeChar(MasterServer.CHAR_REGISTER);
		     
		      // register with covered tables
		      String tablesString = "";
		      for(String s: tables){
		    	  tablesString += s;
		    	  tablesString += ";";
		      }
		      output.writeUTF(tablesString);
		      DataInputStream input = new DataInputStream(masterSocket.getInputStream());
		      do{	
				switch(input.readChar()){
					case MasterServer.CHAR_BLOOMFILTER:
						send(output, input.readUTF(), true);
						break;
					case MasterServer.CHAR_TUPLES:
						send(output, input.readUTF(), false);
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

	private void send(DataOutputStream output, String query, boolean bloomed) {
		
	}
}
