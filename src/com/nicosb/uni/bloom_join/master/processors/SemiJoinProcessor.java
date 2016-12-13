package com.nicosb.uni.bloom_join.master.processors;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

import javax.sql.RowSet;

import com.nicosb.uni.bloom_join.master.MasterServer;

public class SemiJoinProcessor extends JoinProcessor {

	public SemiJoinProcessor(HashMap<String, String> joinAttrs, String query, MasterServer master, String[] tables) {
		super(joinAttrs, query, master, tables);
	}
	
	@Override
	protected void joinRowSets(){
		try{
			for(RowSet rs: rowSets){
				jrs.addRowSet(rs, 1);
			}
			HashSet<Integer> hs_int = new HashSet<>();
			HashSet<String> hs_str = new HashSet<>();
			
			jrs.beforeFirst();
			String type = jrs.getMetaData().getColumnClassName(1);
			while(jrs.next()){
				if(type.equals("java.lang.String")){
					hs_str.add(jrs.getString(1));
				}
				else{
					hs_int.add(jrs.getInt(1));
				}
			}
			if(type.equals("java.lang.String")){
				String[] vals = new String[hs_str.size()];
				hs_str.toArray(vals);
				master.sendIndices(vals);
			}				
			else{
				Integer[] vals = new Integer[hs_int.size()];
				hs_int.toArray(vals);
				master.sendIndices(vals);
			}
			master.currentAssignment.setBloom(true);
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
