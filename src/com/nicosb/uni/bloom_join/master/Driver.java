package com.nicosb.uni.bloom_join.master;

import java.io.IOException;

import com.nicosb.uni.bloom_join.exception.InvalidQueryException;

public class Driver {
	public static void main(String... args){
		MasterServer master = new MasterServer();
		try {
			master.run("localhost", 63843);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
