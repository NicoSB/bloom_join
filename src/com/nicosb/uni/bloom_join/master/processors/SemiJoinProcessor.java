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
			HashSet<String> hs = new HashSet<>();
			jrs.beforeFirst();
			while(jrs.next()){
				hs.add(jrs.getString(1));
			}
			String[] vals = new String[hs.size()];
			hs.toArray(vals);
			master.currentAssignment.setBloom(true);
			master.sendIndices(vals);
		} catch (SQLException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
