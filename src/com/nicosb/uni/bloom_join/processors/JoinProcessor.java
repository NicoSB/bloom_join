package com.nicosb.uni.bloom_join.processors;

import java.sql.SQLException;
import java.util.HashMap;

import com.nicosb.uni.bloom_join.CustomLog;
import com.nicosb.uni.bloom_join.MasterServer;

public class JoinProcessor extends BasicJoinProcessor{

	public JoinProcessor(HashMap<String, String> joinAttrs, String query, MasterServer master, String[] tables) {
		super(joinAttrs, query, master, tables);
	}

	@Override
	protected void joinRowSets() {
		try {
			CustomLog.println("Joining results....");
			for(int i = 0; i < rowSets.size(); i++){
				rowSets.get(i).beforeFirst();
				int col = getJoinAttrIndex(indices.get(i));
				rowSets.get(i).setMatchColumn(col);
				jrs.addRowSet(rowSets.get(i));
			}
			jrs.beforeFirst();
			printFinalLog();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void printFinalLog() throws SQLException {
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
	}
}
