package com.dp.nebula.wormhole.engine.monitor;

import java.util.HashMap;
import java.util.Map;

import com.dp.nebula.wormhole.engine.storage.Statistics;

public class RealtimeMonitorInfo {
	
	private long readSuccessLines;
	private long readFailedLines;
	
	private Map<String, Statistics> smcMap;
	
	private Map<String, Long> writeSuccessLinesMap = new HashMap<String, Long>();
	private Map<String, Long> writeFailedLinesMap = new HashMap<String, Long>();
	
	RealtimeMonitorInfo(int writerNum){
		smcMap = new HashMap<String, Statistics>(writerNum);
		writeSuccessLinesMap = new HashMap<String, Long>(writerNum);
		writeFailedLinesMap = new HashMap<String, Long>(writerNum);
	}

	public long getReadSuccessLines() {
		return readSuccessLines;
	}

	public void setReadSuccessLines(long readSuccessLines) {
		this.readSuccessLines = readSuccessLines;
	}

	public long getReadFailedLines() {
		return readFailedLines;
	}

	public void setReadFailedLines(long readFailedLines) {
		this.readFailedLines = readFailedLines;
	}
	
	public void addStorageMonitorCriteria(String name, Statistics smc){
		smcMap.put(name, smc);
	}
	
	public void addwriteSuccessLines(String name, long number){
		writeSuccessLinesMap.put(name, Long.valueOf(number));
	}
	
	public void addwriteFailedLines(String name, long number){
		writeFailedLinesMap.put(name, number);
	}

	public Map<String, Statistics> getStorageMonitorCriteriaMap() {
		return smcMap;
	}

	public Map<String, Long> getWriteSuccessLinesMap() {
		return writeSuccessLinesMap;
	}

	public Map<String, Long> getWriteFailedLinesMap() {
		return writeFailedLinesMap;
	}
	
	public String getInfo(){
		StringBuilder builder = new StringBuilder();
		Map<String,Statistics> map = getStorageMonitorCriteriaMap();
		builder.append("\n");
		for(String key : map.keySet()){
			Statistics stat = map.get(key);
			builder.append(key).append(" ").append(stat.getPeriodState()).append("\n");
			stat.periodPass();
		}
		return builder.toString();
	}
	
}
