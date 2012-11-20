package com.dp.nebula.wormhole.engine.monitor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.dp.nebula.wormhole.common.DefaultPluginMonitor;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.engine.storage.IStorage;
import com.dp.nebula.wormhole.engine.storage.Statistics;
import com.dp.nebula.wormhole.engine.storage.StorageManager;

public class MonitorManager implements ISourceCounter, ITargetCounter{
	
	private List<IPluginMonitor> readerMonitorPool;
	private Map<String, List<IPluginMonitor>> writerMonitorPoolMap;
	private StorageManager storageManager;
	private long sourceLines;
	private Map<String, Long> targetLinesMap;
	private int writerNum;
	
	//private static final Log s_logger = LogFactory.getLog(MonitorManager.class);

	public MonitorManager(int writerNum){
		targetLinesMap = new HashMap<String, Long>();
		readerMonitorPool = new ArrayList<IPluginMonitor>();
		writerMonitorPoolMap = new HashMap<String, List<IPluginMonitor>>();
		this.writerNum = writerNum;
	}
	
	public void setStorageManager(StorageManager storageManager) {
		this.storageManager = storageManager;
	}
	public void setSourceLines(long sourceLines) {
		this.sourceLines = sourceLines;
	}
	public void setTargetLines(String name, long lines ) {
		if(StringUtils.isEmpty(name)) {
			return;
		}
		targetLinesMap.put(name, Long.valueOf(lines));
	}
	
	public IPluginMonitor newReaderMonitor(){
		IPluginMonitor m = new DefaultPluginMonitor();
		readerMonitorPool.add(m);
		return m;
	}
	
	public IPluginMonitor newWriterMonitor(String name){
		IPluginMonitor m = new DefaultPluginMonitor();
		addWrtierMonitor(name, m);
		return m;
	}
	
	private void addWrtierMonitor(String name, IPluginMonitor m){
		List<IPluginMonitor> ml = writerMonitorPoolMap.get(name);
		if(ml == null) {
			ml = new ArrayList<IPluginMonitor>();
			writerMonitorPoolMap.put(name, ml);
		}
		ml.add(m);
	}
	
	public CompletedMonitorInfo getCompletedMonitorInfo(){
		CompletedMonitorInfo info = new CompletedMonitorInfo(writerNum);
		
		info.setSourceLines(sourceLines);
		long readSuccessLines = 0;
		long readFailedLines = 0;
		for(IPluginMonitor readerMonitor : readerMonitorPool) {
			readSuccessLines += readerMonitor.getSuccessLines();
			readFailedLines += readerMonitor.getFailedLines();
		}
		info.setReadSuccessLines(readSuccessLines);
		info.setReadFailedLines(readFailedLines);
		
		//storage statistics
		Map<String,IStorage> storageMap = storageManager.getStorageMap();
		for(String key : storageMap.keySet()) {
			Statistics stat = storageMap.get(key).getStat();
			info.addStorageMonitorCriteria(key, stat);
		}
	
		for(String key : writerMonitorPoolMap.keySet()){
			long writeSuccessLines = 0;
			long writeFailedLines = 0;
			List<IPluginMonitor> writerMP = writerMonitorPoolMap.get(key);
			for(IPluginMonitor writerMonitor : writerMP) {
				writeSuccessLines += writerMonitor.getSuccessLines();
				writeFailedLines += writerMonitor.getFailedLines();
			}
			info.addwriteSuccessLines(key, writeSuccessLines);
			info.addwriteFailedLines(key, writeFailedLines);
		}
		info.setTargetLinesMap(targetLinesMap);
		return info;
	}
	
	public RealtimeMonitorInfo getRealtimeMonitorInfo(){
		RealtimeMonitorInfo info = new RealtimeMonitorInfo(writerNum);
		
		//storage statistics
		Map<String,IStorage> storageMap = storageManager.getStorageMap();
		for(String key : storageMap.keySet()){
			Statistics stat = storageMap.get(key).getStat();
			info.addStorageMonitorCriteria(key, stat);
		}
		return info;
	}
	
	public String realtimeReport(){
		RealtimeMonitorInfo info = getRealtimeMonitorInfo();
		return info.getInfo();
	}
	
	public String finalReport(){
		CompletedMonitorInfo info = getCompletedMonitorInfo();
		return info.getInfo();
	}
	
	public WormHoleJobInfo getJobInfo(String dataSource, String dataTarget,long time, int status,Date start){
		CompletedMonitorInfo info = getCompletedMonitorInfo();
		String userName = System.getProperty("user.name");
		long totalLines = info.getReadSuccessLines();
		long totalBytes = ((Statistics)info.getStorageMonitorCriteriaMap().values().toArray()[0]).getByteRxTotal();
		return new WormHoleJobInfo(userName, dataSource, dataTarget, start, totalLines, totalBytes, time, status);
	}
	
	public Set<FailedInfo> getFailedInfo(){
		Set<FailedInfo> result = new HashSet<FailedInfo>();
		CompletedMonitorInfo info = getCompletedMonitorInfo();
		long readSuccessLine = info.getReadSuccessLines();
		long writeSuccessLine = 0;
 
		Map<String,Long> writeLinesMap = info.getWriteSuccessLinesMap();
		if(sourceLines == 0){
			sourceLines = readSuccessLine;
		}
		for(String key : writeLinesMap.keySet()){
			writeSuccessLine = writeLinesMap.get(key);
			Long targetLine = targetLinesMap.get(key);
			if(targetLine == null || targetLine == 0) {
				targetLine = writeSuccessLine;
			}
			if(sourceLines != targetLine){
				FailedInfo failInfo = new FailedInfo(key, (int)(sourceLines-targetLine));
				result.add(failInfo);
			}
		}
		return result;
	}
	
	public Boolean isJobSuccess(){
		return getFailedInfo().isEmpty();
	}

}
