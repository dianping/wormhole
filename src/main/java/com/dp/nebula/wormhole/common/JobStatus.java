package com.dp.nebula.wormhole.common;

public enum JobStatus {
	
	RUNNING(-1), 
	SUCCESS(0),
	//200-299 System status
	SUCCESS_WITH_ERROR(201), 
	PARTIAL_FAILED(202),
	FAILED(203),
	CONF_FAILED(204),
	
	//300-599 Reader status
	READ_FAILED(300),
	READ_CONNECTION_FAILED(301),
	PRE_CHECK_FAILED(302),
	READ_OUT_OF_TIME(305),
	READ_DATA_EXCEPTION(306),
	READ_SYSTEM_ERROR(307),
	READ_OPERATOR_INTERVENTION(308),
	

	//600-999 Writer status
	WRITE_FAILED(600),
	WRITE_CONNECTION_FAILED(601),
	PRE_WRITE_FAILED(602),
	ROLL_BACK_FAILED(603),
	POST_WRITE_FAILED(604),
	WRITE_OUT_OF_TIME(605),
	WRITE_DATA_EXCEPTION(606),
	WRITE_SYSTEM_ERROR(607),
	WRITE_OPERATOR_INTERVENTION(608);
	
	private int status;
	
	public static final int PLUGIN_BASE = 1000;
	public static final int WRITER_BASE = 100000;
	
	public int getStatus() {
		return status;
	}

	private JobStatus(int status){
		this.status = status;
	}

	public static JobStatus fromName (String v) {
		try {
			return valueOf(JobStatus.class,v.toUpperCase());
		} catch (Exception e) {
			return null;
		}
	}
	
	public static JobStatus fromStatus (int status) {
		for(JobStatus jobStatus : JobStatus.values()) {
			if(jobStatus.status == status%PLUGIN_BASE) {
				return jobStatus;
			}
		}
		return null;
	}
	
	public boolean isFailed() {
		switch(this)
		{
			case RUNNING:
				return false;
			case SUCCESS: 
				return false;
			case SUCCESS_WITH_ERROR: 
				return false;
			default:
				return true;
		}
	}
}
