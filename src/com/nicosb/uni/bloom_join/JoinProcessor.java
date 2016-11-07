package com.nicosb.uni.bloom_join;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.sql.rowset.JoinRowSet;

import com.sun.rowset.CachedRowSetImpl;
import com.sun.rowset.JoinRowSetImpl;

public class JoinProcessor {
	private ArrayList<ArrayList<Integer>> requestList = new ArrayList<>();
	private ArrayList<String> indices = new ArrayList<>();
	private HashMap<String, String> joinAttrs = new HashMap<>();
	private ArrayList<CachedRowSetImpl> rowSets = new ArrayList<>();
	private JoinRowSet jrs;
	private String currentQuery;
	private MasterServer master;
	boolean occupied = false;
	int count = 0;
	
	/**
	 * This constructor requires, that every table given in tables is actually used.
	 * Otherwise, the processor won't finish!
	 * @param tables
	 */
	public JoinProcessor(HashMap<String, String> joinAttrs, String query, MasterServer master, String... tables){
		count = tables.length;
		currentQuery = query;
		this.master = master;
		for(String t: tables){
			ArrayList<Integer> ints = new ArrayList<>();
			requestList.add(ints);
			indices.add(t);
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
	
	private void joinRowSets() {
		try {
			CustomLog.println("Joining results....");
			for(int i = 0; i < rowSets.size(); i++){
				rowSets.get(i).beforeFirst();
				rowSets.get(i).setMatchColumn(getJoinAttrIndex(indices.get(i)));
				jrs.addRowSet(rowSets.get(i));
			}
			jrs.beforeFirst();
			CustomLog.println("--------------------------------");
			CustomLog.println("|"+currentQuery+"|");
			CustomLog.println("--------------------------------");
			CustomLog.println("-----------RESULT---------------");
			CustomLog.println("----------" + CustomLog.getTraffic(true) + " bytes -----------");
			CustomLog.println("--------------------------------");
			while(jrs.next()){
				for(int i = 1; i <= jrs.getMetaData().getColumnCount(); i++){
					CustomLog.print(jrs.getString(i) + " | ");
				}
				CustomLog.print("\n");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private int getJoinAttrIndex(String table) {
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
}
