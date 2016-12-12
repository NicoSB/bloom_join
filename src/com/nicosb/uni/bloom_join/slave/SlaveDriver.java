package com.nicosb.uni.bloom_join.slave;

import java.util.ArrayList;

public class SlaveDriver {
	public static void main(String... args){
		int masterPort = Integer.valueOf(args[0]);
		ArrayList<String> tables = new ArrayList<>();
		tables.add("numbers");
		tables.add("fives");
		tables.add("thirteens");
		
		SlaveServer ss = new SlaveServer(tables);
		ss.run("localhost", masterPort);		
	}
}
