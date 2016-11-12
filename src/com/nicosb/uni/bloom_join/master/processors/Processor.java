package com.nicosb.uni.bloom_join.master.processors;

public interface Processor {
	public void addRequested(String table, int serverId);
}
