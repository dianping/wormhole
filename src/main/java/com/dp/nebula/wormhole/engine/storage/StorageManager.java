package com.dp.nebula.wormhole.engine.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dp.nebula.wormhole.engine.utils.ReflectionUtil;


public class StorageManager {
	
	private Map<String, IStorage> storageMap = new HashMap<String, IStorage>();

	public StorageManager(List<StorageConf> confList){
		for(StorageConf conf:confList) {
			if(conf == null || conf.getId() == null) {
				continue;
			}
			IStorage storage = ReflectionUtil.createInstanceByDefaultConstructor(conf.getStorageClassName(), IStorage.class);
			if(storage.init(conf.getId(), conf.getLineLimit(), conf.getByteLimit(), conf.getDestructLimit(),conf.getWaitTime())){
				storage.getStat().setPeriodInSeconds(conf.getPeriod());
				storageMap.put(conf.getId(), storage);
			}
		}
	}
	
	public Map<String, IStorage> getStorageMap() {
		return storageMap;
	}
	
	public List<IStorage> getStorageForReader() {
		List<IStorage> result = new ArrayList<IStorage>();
		for(IStorage storage : storageMap.values()) {
			result.add(storage);
		}
		return result;
	}
	
	public IStorage getStorageForWriter(String id){
		return storageMap.get(id);
	}
	
	public void closeInput(){
		for(String key:storageMap.keySet()){
			storageMap.get(key).close();
		}
	}
}
