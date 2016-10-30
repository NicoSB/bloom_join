package com.nicosb.uni.bloom_join;

import java.util.BitSet;
import java.util.LinkedList;
import java.lang.Math;

public class Bloomer{
	public static BitSet bloom(int[] ints, int n, float p){
		int m = (int)Math.ceil((n * Math.log(p))/ Math.log(1.0/(Math.pow(2.0, Math.log(2.0)))));
		int k = (int)(Math.log(2.0)*m/n);

		BitSet bitset = new BitSet(m);
		bitset.clear();

		for(int i: ints){
			for(int j = 0; j < k; j++){
				bitset.set(hash(i, j, m));
			}
		}
		return bitset;
	}
	public static BitSet bloom(LinkedList<Integer> ints, int n, float p){
		int m = (int)Math.ceil((n * Math.log(p))/ Math.log(1.0/(Math.pow(2.0, Math.log(2.0)))));
		int k = (int)(Math.log(2.0)*m/n);

		BitSet bitset = new BitSet(m);
		bitset.clear();

		for(int i: ints){
			for(int j = 0; j < k; j++){
				bitset.set(hash(i, j, m));
			}
		}
		return bitset;
	}
	public static BitSet bloom(int[] ints, int k, int m){

		BitSet bitset = new BitSet(m);

		for(int i: ints){
			for(int j = 0; j < k; j++){
				bitset.set(hash(i, j, m));
			}
		}
		return bitset;
	}
	public static BitSet bloom(LinkedList<Integer> ints, int k, int m){

		BitSet bitset = new BitSet(m);

		for(int i: ints){
			for(int j = 0; j < k; j++){
				bitset.set(hash(i, j, m));
			}
		}
		return bitset;
	}

	private static int hash(int value, int i, int m){
		// TODO: replace with Murmur hash
		return (value*i)%m;
	}

	public static boolean is_in(int value, BitSet b, int n, float p){	
		int m = (int)Math.ceil((n * Math.log(p))/ Math.log(1.0/(Math.pow(2.0, Math.log(2.0)))));
		int k = (int)(Math.log(2.0)*m/n);

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