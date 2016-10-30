package com.nicosb.uni.bloom_join;

import java.util.ArrayList;
import java.util.BitSet;

public class BloomProcessor {
	private ArrayList<ArrayList<Integer>> requestList = new ArrayList<>();
	private ArrayList<String> indices = new ArrayList<>();
	private ArrayList<BitSet> bloomFilters = new ArrayList<>();
	boolean occupied = false;
	int count = 0;
	
	/**
	 * This constructor requires, that every table given in tables is actually used.
	 * Otherwise, the processor won't finish!
	 * @param tables
	 */
	public BloomProcessor(String... tables){
		count = tables.length;
		for(String t: tables){
			ArrayList<Integer> ints = new ArrayList<>();
			requestList.add(ints);
			indices.add(t);
			bloomFilters.add(new BitSet());
		}
	}
	
	public void addRequested(String table, int serverId){
		while(occupied){};
		occupied = true;
		int index = getIndex(table);
		requestList.get(index).add(serverId);
		occupied = false;
	}
	
	private boolean removeRequested(String table, int serverId){
		int index = getIndex(table);
		
		int y = 0;
		for(int id: requestList.get(index)){
			if(id == serverId) break;
			y++;
		}
		requestList.get(index).remove(y);
		
		if(requestList.get(index).size() == 0){
			count--;
			if(count == 0){
				return true;
			}
		}
		
		return false;	
	}

	private int getIndex(String table) {
		int index = -1;
		for(String str: indices){
			index++;
			if(str.equals(table)) return index;
		}
		return -1;
	}
	
	public BitSet ORJoin(String table, int serverId, byte[] bytes){		
		while(occupied){};
		occupied = true;
		boolean finished = removeRequested(table, serverId);
		int index = getIndex(table);
		BitSet bf = BitSet.valueOf(bytes);
		BitSet bs = bloomFilters.get(index);
		
		if(bs == null)	{bs = new BitSet(bytes.length * 8);}
		bs.or(bf);
		
		if(finished) {return ANDJoin();}
		occupied = false;
		return null;
	}

	private BitSet ANDJoin() {
		BitSet bs = bloomFilters.get(0);
		for(BitSet b: bloomFilters){
			bs.and(b);
		}

		occupied = false;
		return bs;
	}
	
	public BitSet getBloomFilter(String table){
		return bloomFilters.get(getIndex(table));
	}
}
