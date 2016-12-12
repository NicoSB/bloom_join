package com.nicosb.uni.bloom_join.slave;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.LinkedList;

import com.nicosb.uni.bloom_join.EnglishNumberToWords;

import util.hash.MurmurHash3;


public class Bloomer{

	public static BitSet bloom_str(LinkedList<String> strings, int k, int m){

		BitSet bitset = new BitSet(m);

		for(String str: strings){
			for(int j = 0; j < k; j++){
				bitset.set(hash(str, j, m));
			}
		}
		return bitset;
	}
	
	public static BitSet bloom_int(LinkedList<Integer> ints, int k, int m){

		BitSet bitset = new BitSet(m);
		for(int i: ints){
			for(int j = 0; j < k; j++){
				bitset.set(hash(i, j, m));
			}
		}
		return bitset;
	}

	private static int hash(String value, int i, int m){
		if(value != null){
			return Math.abs(MurmurHash3.murmurhash3_x86_32(value, 0, value.length(), i*i) % m);
		}
		else{
			return 0;
		}
	}

	private static int hash(int value, int i, int m){
		byte[] bytes = ByteBuffer.allocate(Integer.SIZE/8).putInt(i).array();
		if(bytes != null){
			return Math.abs(MurmurHash3.murmurhash3_x86_32(bytes, 0, bytes.length, i*i) % m);
		}
		else{
			return 0;
		}
	}

	public static boolean is_in(String value, BitSet b, int k, int m){	
		for(int i = 0; i < k; i++){
			if(!b.get(hash(value, i, m))){
				return false;
			}
		}
	
		return true;
	}

	public static boolean is_in(int value, BitSet b, int k, int m){	
		for(int i = 0; i < k; i++){
			if(!b.get(hash(value, i, m))){
				return false;
			}
		}
	
		return true;
	}
}