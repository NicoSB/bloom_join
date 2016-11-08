package com.nicosb.uni.bloom_join.processors;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.sql.rowset.JoinRowSet;

import com.nicosb.uni.bloom_join.MasterServer;
import com.sun.rowset.CachedRowSetImpl;
import com.sun.rowset.JoinRowSetImpl;

public class BasicJoinProcessor implements Processor {
	protected ArrayList<ArrayList<Integer>> requestList = new ArrayList<>();
	protected ArrayList<String> indices = new ArrayList<>();
	protected HashMap<String, String> joinAttrs = new HashMap<>();
	protected ArrayList<CachedRowSetImpl> rowSets = new ArrayList<>();
	protected JoinRowSet jrs;
	protected String currentQuery;
	protected MasterServer master;
	boolean occupied = false;
	int count = 0;
	
	/**
	 * This constructor requires, that every table given in tables is actually used.
	 * Otherwise, the processor won't finish!
	 * @param tables
	 */
	public BasicJoinProcessor(HashMap<String, String> joinAttrs, String query, MasterServer master, String... tables){
		count = tables.length;
		currentQuery = query;
		this.master = master;
		
		for(String t: tables){
			ArrayList<Integer> ints = new ArrayList<>();
			requestList.add(ints);
			indices.add(t.toLowerCase());
			rowSets.add(null);
			this.joinAttrs = joinAttrs;
			try {
				jrs = new JoinRowSetImpl();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void addRequested(String table, int serverId){
		while(occupied){};
		occupied = true;
		int index = getIndex(table);
		requestList.get(index).add(serverId);
		occupied = false;
	}
	
	private int getIndex(String table) {
		table = table.toLowerCase();
		int index = -1;
		for(String str: indices){
			index++;
			if(str.equals(table)) return index;
		}
		return -1;
	}
	
	public void addRowSet(String table, int serverId, CachedRowSetImpl crsi){
		boolean finished = removeRequested(table, serverId);
		int index = getIndex(table);
		if(rowSets.get(index) == null){
			rowSets.set(getIndex(table), crsi);	
		}
		else{
			try {
				crsi.beforeFirst();
				while(crsi.next()){
					rowSets.get(index).moveToInsertRow();
					for(int i = 1; i <= crsi.getMetaData().getColumnCount(); i++){
						rowSets.get(index).updateString(i, crsi.getString(i));
					}
					rowSets.get(index).insertRow();
					rowSets.get(index).moveToCurrentRow();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		if(finished){	
			joinRowSets();
		}
	}
	

	/**
	 * returns the index of the designated join attribute
	 * @param table table name
	 * @return index of the attribute
	 */
	protected int getJoinAttrIndex(String table) {
		ResultSetMetaData rsm;
		try {
			rsm = rowSets.get(getIndex(table)).getMetaData();
			for(int i = 1; i <= rsm.getColumnCount(); i++){
				if(rsm.getColumnName(i).equals(joinAttrs.get(table))){
					return i;
				}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return 0;
	}

	public CachedRowSetImpl getRowSet(String table){
		try{
			return rowSets.get(getIndex(table));
		}catch(IndexOutOfBoundsException e){
			return null;
		}
	}
	
	protected boolean removeRequested(String table, int serverId){
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

	protected void joinRowSets(){}
}
