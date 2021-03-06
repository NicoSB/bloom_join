package com.nicosb.uni.bloom_join.master.processors;

import java.sql.SQLException;
import java.util.HashMap;

import com.nicosb.uni.bloom_join.CustomLog;
import com.nicosb.uni.bloom_join.master.MasterServer;

/**
 * Provides the processor that joins the actual tuples in the last phase. 
 * Furthermore, it prints the final log.
 * @author Nico
 *
 */
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
			master.lock = false;
			if(master.currentAssignment.isEvaluating()){
				master.pop();
				System.out.println(master.getErrorRate() + "\t" + CustomLog.getTraffic(true));
				if(master.evalFinished()){
					master.currentAssignment.setEvaluating(false);
					master.lock = false;
				}
			}
			else{
				printFinalLog();
				master.lock = false;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * The final log is always printed to the console, unless the "-e" is activated
	 * This method prints the executed query, the needed traffic and the tuples 
	 * if the console output is not disabled.
	 * 
	 * @throws SQLException
	 */
	private void printFinalLog() throws SQLException {
		CustomLog.println("--------------------------------", true);
		CustomLog.println("|"+currentQuery+"|", true);
		CustomLog.println("--------------------------------", true);
		CustomLog.println("-----------RESULT---------------", true);
		CustomLog.println("----------" + CustomLog.getTraffic(true) + " bytes -----------", true);
		CustomLog.println("--------------------------------", true);
		while(jrs.next()){
			for(int i = 1; i <= jrs.getMetaData().getColumnCount(); i++){
				CustomLog.print(jrs.getString(i) + " | ");
			}
			CustomLog.print("\n");
		}
	}
}
