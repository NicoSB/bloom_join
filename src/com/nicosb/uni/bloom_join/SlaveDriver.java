package com.nicosb.uni.bloom_join;

import java.util.ArrayList;

public class SlaveDriver {
	public static void main(String... args){
		int masterPort = Integer.valueOf(args[0]);
		ArrayList<String> tables = new ArrayList<>();
		tables.add("country");
		tables.add("geo_Desert");
		
		SlaveServer ss = new SlaveServer(tables);
		ss.run("localhost", masterPort);		
	}
}
