package com.nicosb.uni.bloom_join.master;

public class QueryValidator {
	public static boolean validate(String query){
		String pattern = "select [*\\w\\s]+ from [\\s\\w\\.]+ [join [\\s\\w\\.]+ on [\\s\\w\\._]+= [\\s\\w\\._]+]+";
		// TODO: implement table and column checking
		return query.matches(pattern);
	}
}
