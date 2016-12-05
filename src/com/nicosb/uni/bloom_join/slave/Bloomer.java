package com.nicosb.uni.bloom_join.slave;

import java.util.BitSet;
import java.util.LinkedList;

import util.hash.MurmurHash3;


public class Bloomer{

	public static BitSet bloom(LinkedList<String> strings, int k, int m){

		BitSet bitset = new BitSet(m);

		for(String str: strings){
			for(int j = 0; j < k; j++){
				bitset.set(hash(str, j, m));
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

	public static boolean is_in(String value, BitSet b, int k, int m){	
		for(int i = 0; i < k; i++){
			if(!b.get(hash(value, i, m))){
				return false;
			}
		}
	
		return true;
	}
}