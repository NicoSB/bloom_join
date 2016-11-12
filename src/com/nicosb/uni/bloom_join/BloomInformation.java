package com.nicosb.uni.bloom_join;

public class BloomInformation {
	private int k;
	private int m;
	
	public BloomInformation(QueryInformation qi, float error_rate){
		this(qi.getMaxJoinSize(qi), error_rate);
		// TODO actually calculate the set size
	}	

	public BloomInformation(int k, int m){
		k = 12;
		m = 800;
		// TODO actually calculate the set size
	}	
	
	public BloomInformation(int n, float p){
		m = (int)Math.ceil((n * Math.log(p))/ Math.log(1.0/(Math.pow(2.0, Math.log(2.0)))));
		k = (int)(Math.log(2.0)*m/n);
	}
	
	public int getHashCount(){
		return k;
	}
	
	public int getFilterSize(){
		return m;
	}
}
