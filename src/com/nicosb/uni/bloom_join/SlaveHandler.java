package com.nicosb.uni.bloom_join;

import java.net.Socket;

public class SlaveHandler implements Runnable {

	private Socket socket;
	
	public SlaveHandler(Socket socket) {
		super();
		this.socket = socket;
	}


	@Override
	public void run() {
		// TODO Auto-generated method stub
		
	}

}
