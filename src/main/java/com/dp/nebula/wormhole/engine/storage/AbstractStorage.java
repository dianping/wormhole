package com.dp.nebula.wormhole.engine.storage;

public abstract class AbstractStorage implements IStorage{
	
	private Boolean pushClosed;
	private int destructLimit;
	private Statistics stat;
	
	public Boolean getPushClosed() {
		return pushClosed;
	}
	public void setPushClosed(Boolean pushClosed) {
		this.pushClosed = pushClosed;
	}
	public int getDestructLimit() {
		return destructLimit;
	}
	public void setDestructLimit(int destructLimit) {
		this.destructLimit = destructLimit;
	}
	public Statistics getStat() {
		return stat;
	}
	public void setStat(Statistics stat) {
		this.stat = stat;
	}
}
