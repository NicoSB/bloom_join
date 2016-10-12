package com.nicosb.uni.bloom_join;

public class QueryValidator {
	public static boolean validate(String query){
		String pattern = "SELECT [*\\w\\s]+ FROM [\\s\\w\\.]+ [JOIN [\\s\\w\\.]+ ON [\\s\\w\\._]+= [\\s\\w\\._]+]+";
		// TODO: implement table and column checking
		return query.matches(pattern);
	}
}
