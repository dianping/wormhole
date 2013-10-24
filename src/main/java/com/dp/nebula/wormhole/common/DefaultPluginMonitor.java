package com.dp.nebula.wormhole.common;

import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;

public class DefaultPluginMonitor implements IPluginMonitor{
	
	private long successLines;
	private long failedLines;

	@Override
	public long getSuccessLines() {
		return successLines;
	}

	@Override
	public long getFailedLines() {
		return failedLines;
	}

	@Override
	public void increaseSuccessLines() {
		increaseSuccessLine(1);
	}

	@Override
	public void increaseSuccessLine(long lines) {
		successLines += lines;	
	}

	@Override
	public void increaseFailedLines() {
		increaseFailedLines(1);
	}

	@Override
	public void increaseFailedLines(long lines) {
		failedLines += lines;
	}

}
