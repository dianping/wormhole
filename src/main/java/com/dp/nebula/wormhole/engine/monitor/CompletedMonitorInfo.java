package com.dp.nebula.wormhole.engine.monitor;

import java.util.HashMap;
import java.util.Map;

import com.dp.nebula.wormhole.engine.storage.Statistics;

public class CompletedMonitorInfo extends RealtimeMonitorInfo{
	
	private long sourceLines;
	private Map<String, Long> targetLinesMap;
	
	CompletedMonitorInfo(int writerNum){
		super(writerNum);
		targetLinesMap = new HashMap<String, Long>(writerNum);
	}
	
	public long getSourceLines() {
		return sourceLines;
	}
	
	public void setSourceLines(long sourceLines) {
		this.sourceLines = sourceLines;
	}

	public void addTargetLines(String name, Long number){
		targetLinesMap.put(name, number);
	}

	public void setTargetLinesMap(Map<String, Long> targetLinesMap) {
		this.targetLinesMap = targetLinesMap;
	}

	public Map<String, Long> getTargetLinesMap() {
		return targetLinesMap;
	}

	@Override
	public String getInfo(){
		StringBuilder builder = new StringBuilder();
		Map<String,Statistics> map = getStorageMonitorCriteriaMap();
		builder.append("\n");
		for(String key : map.keySet()){
			Statistics stat = map.get(key);
			builder.append(key).append(":").append(stat.getTotalStat()).append("\n");
		}
		return builder.toString();
	}
}
