package com.nicosb.uni.bloom_join;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class QueryInformation {
	private String[] tables;
	private HashMap<String, String> joinAttributes;
		
	public QueryInformation(String query) throws InvalidQueryException{
		if(!QueryValidator.validate(query)){
			throw new InvalidQueryException();
		}
		else{
			tables = extractTables(query);
			joinAttributes = extractJoinAttributes(query);
		}
	}

	private HashMap<String, String> extractJoinAttributes(String query) {
		HashMap<String, String> attrs = new HashMap<>();
		String attributesSubstring = query;
		int index;
		Pattern p = Pattern.compile("[\\w_]+.[\\w_]+");
		Matcher m;
		while((index = attributesSubstring.indexOf("ON ")) != -1){
			attributesSubstring = attributesSubstring.substring(index + "ON ".length());
			m = p.matcher(attributesSubstring); 
			if(m.find()){
				String attr = m.group();
				attrs.put(attr, attr.substring(0, attr.indexOf(".")));
				attributesSubstring = attributesSubstring.substring(attributesSubstring.indexOf("=") + 1).trim();
				m = p.matcher(attributesSubstring);
				if(m.find()){
					attr = m.group();
					attrs.put(attr, attr.substring(0, attr.indexOf(".")));
				}
			}
		}
		return attrs;
	}

	private String[] extractTables(String query) {
		
		String table1 = query.substring(query.indexOf("FROM ") + "FROM ".length(), query.indexOf("JOIN")).trim();
		String joinSubstring = query;
		ArrayList<String> join_tables = new ArrayList<>();
		join_tables.add(table1);
		int index;
		while((index = joinSubstring.indexOf("JOIN ")) != -1){
			joinSubstring = joinSubstring.substring(index + "JOIN ".length()).trim();
			join_tables.add(joinSubstring.substring(0, joinSubstring.indexOf(" ")));
			joinSubstring = joinSubstring.substring(joinSubstring.indexOf(" ")).trim();
		}
		
		String[] ret = new String[join_tables.size()];
		join_tables.toArray(ret);
		return ret;
	}

	public String[] getTables() {
		return tables;
	}

	public void setTables(String[] tables) {
		this.tables = tables;
	}

	public HashMap<String, String> getJoinAttributes() {
		return joinAttributes;
	}

	public void setJoinAttributes(HashMap<String, String> joinAttributes) {
		this.joinAttributes = joinAttributes;
	}
	
	
}
