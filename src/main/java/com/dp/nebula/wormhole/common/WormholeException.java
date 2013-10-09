package com.dp.nebula.wormhole.common;

public class WormholeException extends RuntimeException{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5961255124852822007L;
	private int statusCode;
	private String pluginID;
	
	
	public int getStatusCode() {
		return statusCode;
	}

	public void setStatusCode(int statusCode) {
		this.statusCode = statusCode;
	}

	public String getPluginID() {
		return pluginID;
	}

	public void setPluginID(String pluginID) {
		this.pluginID = pluginID;
	}

	public WormholeException(Exception e, int jobStatus) {
		super(e);
		this.statusCode = jobStatus;
	}
	
	public WormholeException(String m, int jobStatus) {
		super(m);
		this.statusCode = jobStatus;
	}
	
	public WormholeException(Exception e, int jobStatus, String pluginID) {
		super(e);
		this.statusCode = jobStatus;
		this.pluginID = pluginID;
	}
	
	public WormholeException(String m, int jobStatus, String pluginID) {
		super(m);
		this.statusCode = jobStatus;
		this.pluginID = pluginID;
	}
	
	public WormholeException(int jobStatus) {
		this.statusCode = jobStatus;
	}
}
