package com.dp.nebula.wormhole.engine.storage;

public class StorageConf {
	
	private String id;
	private String storageClassName;
	private int lineLimit;
	private int byteLimit;
	private int destructLimit;
	private int period;
	private int waitTime;
	
	

	public String getId() {
		return id;
	}



	public void setId(String id) {
		this.id = id;
	}



	public String getStorageClassName() {
		return storageClassName;
	}



	public void setStorageClassName(String storageClassName) {
		this.storageClassName = storageClassName;
	}



	public int getLineLimit() {
		return lineLimit;
	}



	public void setLineLimit(int lineLimit) {
		this.lineLimit = lineLimit;
	}



	public int getByteLimit() {
		return byteLimit;
	}



	public void setByteLimit(int byteLimit) {
		this.byteLimit = byteLimit;
	}



	public int getDestructLimit() {
		return destructLimit;
	}



	public void setDestructLimit(int destructLimit) {
		this.destructLimit = destructLimit;
	}



	public int getPeriod() {
		return period;
	}



	public void setPeriod(int period) {
		this.period = period;
	}



	public int getWaitTime() {
		return waitTime;
	}



	public void setWaitTime(int waitTime) {
		this.waitTime = waitTime;
	}



	public StorageConf(String id, String storageClassName, int lineLimit,
			int byteLimit, int destructLimit, int period, int waitTime) {
		super();
		this.id = id;
		this.storageClassName = storageClassName;
		this.lineLimit = lineLimit;
		this.byteLimit = byteLimit;
		this.destructLimit = destructLimit;
		this.period = period;
		this.waitTime = waitTime;
	}



	public StorageConf() {
	}
}
