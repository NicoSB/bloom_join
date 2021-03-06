package com.nicosb.uni.bloom_join;

import com.nicosb.uni.bloom_join.master.processors.BloomProcessor;
import com.nicosb.uni.bloom_join.master.processors.JoinProcessor;
import com.nicosb.uni.bloom_join.master.processors.Processor;
import com.nicosb.uni.bloom_join.master.processors.SemiJoinProcessor;

/**
 * The Assignment class represents the server's current assignment. It stores crucial information and objects,
 * such as information about the query and bloomfilter, as well as the processors needed in further steps
 * 
 * @author Nico
 *
 */
public class Assignment {
	public QueryInformation cachedQuery;
	private BloomProcessor activeProcessor;
	private SemiJoinProcessor semiJoinProcessor;
	private JoinProcessor joinProcessor;
	private BloomInformation bloomInformation;
	private boolean evaluating = false;
	private boolean bloom = true;
	
	public Assignment(Assignment a){
		if(a != null) this.evaluating = a.isEvaluating();
	}
	public QueryInformation getCachedQuery() {
		return cachedQuery;
	}

	public void setCachedQuery(QueryInformation cachedQuery) {
		this.cachedQuery = cachedQuery;
	}

	public BloomProcessor getActiveProcessor() {
		return activeProcessor;
	}

	public void setActiveProcessor(BloomProcessor activeProcessor) {
		this.activeProcessor = activeProcessor;
	}

	public JoinProcessor getJoinProcessor() {
		return joinProcessor;
	}

	public void setJoinProcessor(JoinProcessor joinProcessor) {
		this.joinProcessor = joinProcessor;
	}

	public SemiJoinProcessor getSemiJoinProcessor() {
		return semiJoinProcessor;
	}

	public void setSemiJoinProcessor(SemiJoinProcessor semiJoinProcessor) {
		this.semiJoinProcessor = semiJoinProcessor;
	}

	public boolean isBloom() {
		return bloom;
	}

	public void setBloom(boolean bloom) {
		this.bloom = bloom;
	}

	public void addRequested(Processor proc, String table, int serverId){
		proc.addRequested(table, serverId);
	}

	public void addRequested(String table, int serverId){
		if(activeProcessor != null) activeProcessor.addRequested(table, serverId);
		if(joinProcessor != null) joinProcessor.addRequested(table, serverId);
		if(semiJoinProcessor != null) semiJoinProcessor.addRequested(table, serverId);
	}

	public BloomInformation getBloomInformation(){
		return bloomInformation;
	}
	
	public void setBloomInformation(BloomInformation bloomInformation){
		this.bloomInformation = bloomInformation;
	}

	public boolean isEvaluating() {
		return evaluating;
	}

	public void setEvaluating(boolean evaluating) {
		this.evaluating = evaluating;
	}
}