package com.nicosb.uni.bloom_join.test;

import java.util.HashMap;

import org.junit.Assert;
import org.junit.Test;

import com.nicosb.uni.bloom_join.QueryInformation;
import com.nicosb.uni.bloom_join.exception.InvalidQueryException;

public class QueryInformationTest {

	@Test
	public void extractTables_extractsAllTablesSingleJoin() {
		String query = "SELECT * FROM table1 JOIN table2 ON table1.a = table2.b";
		String[] tables = {"table1", "table2"};
		try {
			QueryInformation qi = new QueryInformation(query);
			Assert.assertArrayEquals(qi.getTables(), tables);
		} catch (InvalidQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	@Test
	public void extractTables_extractsAllTablesMultipleJoins() {
		String query = "SELECT * FROM table1 JOIN table2 ON table1.a = table2.b JOIN table3 ON table1.a = table3.c";
		String[] tables = {"table1", "table2", "table3"};
		try {
			QueryInformation qi = new QueryInformation(query);
			Assert.assertArrayEquals(qi.getTables(), tables);
		} catch (InvalidQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void extractAttributes_extractsAllAttributes(){
		String query = "SELECT * FROM table1 JOIN table2 ON table1.a = table2.b JOIN table3 ON table1.a = table3.c";
		HashMap<String, String> expected = new HashMap<>();
		expected.put("table1", "a");
		expected.put("table2", "b");
		expected.put("table3", "c");
		
		QueryInformation qi;
		try {
			qi = new QueryInformation(query);

			Assert.assertEquals(qi.getJoinAttributes().get("table1"), expected.get("table1"));
			Assert.assertEquals(qi.getJoinAttributes().get("table2"), expected.get("table2"));
			Assert.assertEquals(qi.getJoinAttributes().get("table3"), expected.get("table3"));
		} catch (InvalidQueryException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
