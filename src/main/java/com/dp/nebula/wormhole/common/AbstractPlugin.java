package com.dp.nebula.wormhole.common;

import java.util.Map;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPlugin;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;

public abstract class AbstractPlugin implements IPlugin{
	
	private IParam param;
	
	private IPluginMonitor monitor;

	private String pluginName;

	private String pluginVersion;
	
	public static final String PLUGINID = "pluginID";
	
	public void setParam(IParam param){
		this.param = param;
	}
	
	public IParam getParam(){
		return param;
	}
	
	public void setMonitor(IPluginMonitor monitor){
		this.monitor = monitor;
	}
	
	public IPluginMonitor getMonitor(){
		return monitor;
	}

	public String getPluginName() {
		return pluginName;
	}

	public void setPluginName(String pluginName) {
		this.pluginName = pluginName;
	}

	public String getPluginVersion() {
		return pluginVersion;
	}

	public void setPluginVersion(String pluginVersion) {
		this.pluginVersion = pluginVersion;
	}
	
	@Override
	public void init() {		
	}

	@Override
	public void connection() {		
	}

	@Override
	public void finish() {		
	}

	@Override
	public Map<String, String> getMonitorInfo() {
		return null;
	}
}
