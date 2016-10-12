package com.nicosb.uni.bloom_join.test;

import org.junit.Assert;
import org.junit.Test;

import com.nicosb.uni.bloom_join.QueryValidator;

public class ValidatorTest {

	@Test
	public void validate_validatesSingleJoin() {
		String query = "SELECT * FROM table JOIN join_table ON table.a = join_table.b";
		Assert.assertTrue(QueryValidator.validate(query));
	}

	@Test
	public void validate_validatesMultipleJoins() {
		String query = "SELECT * FROM table1 JOIN table2 ON table1.a = table2.b JOIN table3 ON table1.a = table3.c";
		Assert.assertTrue(QueryValidator.validate(query));
	}
	
	

}
