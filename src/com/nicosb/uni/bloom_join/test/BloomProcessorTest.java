package com.nicosb.uni.bloom_join.test;

import java.util.BitSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.nicosb.uni.bloom_join.processors.BloomProcessor;

public class BloomProcessorTest {
	BloomProcessor prc;
	@Before
	public void setup(){
		String[] tables = {"table1", "table2", "table3"};
		prc = new BloomProcessor(tables);
	}
	@Test
	public void ORJoin_joinsCorrectly() {
		byte[] bytes1 = { Byte.parseByte("01100110", 2)};
		byte[] bytes2 = { Byte.parseByte("00010000", 2)};
		byte[] bytes3 = { Byte.parseByte("00000001", 2)};

		prc.addRequested("table1", 13);
		prc.addRequested("table2", 13);

		prc.addRequested("table1", 12);
		prc.addRequested("table3", 12);
		
		prc.addRequested("table1", 11);
		
		prc.ORJoin("table1", 13, bytes1);
		BitSet b1 = BitSet.valueOf(bytes1);
		Assert.assertEquals(prc.getBloomFilter("table1"), b1);

		prc.ORJoin("table1", 12, bytes2);
		BitSet b2 = BitSet.valueOf(bytes2);
		b2.or(b1);
		Assert.assertEquals(prc.getBloomFilter("table1"), b2);
		
		prc.ORJoin("table1", 11, bytes3);
		BitSet b3 = BitSet.valueOf(bytes3);
		b3.or(b2);
		Assert.assertEquals(b3, prc.getBloomFilter("table1"));
		
	}
	
	@Test 
	public void ANDJoin_isTriggeredOnEmpty(){
		byte[] bytes1 = { Byte.parseByte("01100110", 2)};
		byte[] bytes2 = { Byte.parseByte("00010000", 2)};
		
		prc.addRequested("table1", 13);
		prc.addRequested("table2", 12);
		prc.addRequested("table3", 3);

		prc.ORJoin("table1", 13, bytes1);
		
		BitSet b1 = BitSet.valueOf(bytes1);

		b1.and(BitSet.valueOf(bytes1));
		prc.ORJoin("table3", 3, bytes1);

		b1.and(BitSet.valueOf(bytes2));
		Assert.assertEquals(b1, prc.ORJoin("table2", 12, bytes2));
		
	}

}
