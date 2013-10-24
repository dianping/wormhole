package com.dp.nebula.wormhole.common.config;

import com.dp.nebula.wormhole.common.interfaces.IParam;

public class JobPluginConf {
	
	private String pluginName;
	private String id;
	private IParam pluginParam;

	public JobPluginConf(String pluginName, IParam pluginParam, String id) {
		super();
		this.pluginName = pluginName;
		this.pluginParam = pluginParam;
		this.id = id;
	}
	
	public JobPluginConf() {
	}

	public String getPluginName() {
		return pluginName;
	}

	public IParam getPluginParam() {
		return pluginParam;
	}

	public void setPluginName(String pluginName) {
		this.pluginName = pluginName;
	}

	public void setPluginParam(IParam pluginParam) {
		this.pluginParam = pluginParam;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	
}
