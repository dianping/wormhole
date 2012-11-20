package com.dp.nebula.wormhole.common.interfaces;

public interface IPluginMonitor {
	
	long getSuccessLines();
	
	long getFailedLines();
	
	void increaseSuccessLines();
	
	void increaseSuccessLine(long lines);
	
	void increaseFailedLines();
	
	void increaseFailedLines(long lines);

}
