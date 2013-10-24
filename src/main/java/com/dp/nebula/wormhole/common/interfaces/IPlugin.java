package com.dp.nebula.wormhole.common.interfaces;

import java.util.Map;

public interface IPlugin {
	
	void setParam(IParam param);
	
	IParam getParam();
	
	void setMonitor(IPluginMonitor monitor);
	
	IPluginMonitor getMonitor();
	
	void init();
	
	void connection();
	
	void finish();
	
	Map<String, String> getMonitorInfo();

}
