package com.nicosb.uni.bloom_join;

public class Driver {
	public static void main(String... args){
		MasterServer master = new MasterServer();
		master.run("localhost", 0);
	}
}
