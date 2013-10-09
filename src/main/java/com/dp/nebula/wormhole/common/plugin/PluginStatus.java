package com.dp.nebula.wormhole.common.plugin;

public enum PluginStatus {
	FAILURE(-1),
	SUCCESS(0),
	CONNECT(1),
	READ(2),
	READ_OVER(3),
	WRITE(4),
	WRITE_OVER(5),	
	WAITING(6);
	
	private int status;
	
	private PluginStatus(int status) {
		this.status = status;
	}
	
	public int value(){
		return this.status;
	}

}
