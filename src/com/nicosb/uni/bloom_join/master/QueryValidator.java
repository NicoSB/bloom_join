package com.nicosb.uni.bloom_join.master;

public class QueryValidator {
	/**
	 * validates whether the query complies to a simple pattern
	 * 
	 * @param query
	 * @return true if the query is valid, false otherwise
	 * 
	 * 	 */
	public static boolean validate(String query){
		String pattern = "select [*\\w\\s]+ from [\\s\\w\\.]+ [join [\\s\\w\\.]+ on [\\s\\w\\._]+= [\\s\\w\\._]+]+";
		
		return query.matches(pattern);
	}
}
