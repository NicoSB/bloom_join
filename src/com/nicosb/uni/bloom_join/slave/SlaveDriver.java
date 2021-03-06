package com.nicosb.uni.bloom_join.slave;

import java.util.ArrayList;

public class SlaveDriver {
	public static void main(String... args){
		int masterPort = Integer.valueOf(args[0]);
		ArrayList<String> tables = new ArrayList<>();
		if(args.length < 2){
			tables.add("numbers");
			tables.add("numbers2");
			tables.add("fives");
			tables.add("thirteens");
		}
		else{
			for(int i = 1; i < args.length; i++){
				tables.add(args[i]);
			}
		}
		
		SlaveServer ss = new SlaveServer(tables);
		ss.run("localhost", masterPort);		
	}
}