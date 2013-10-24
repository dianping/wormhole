package com.dp.nebula.wormhole.engine.common;

import java.util.Map;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;

public class FakeWriter extends AbstractPlugin implements IWriter{

	@Override
	public void init() {}

	@Override
	public void connection() {}

	@Override
	public void finish() {}

	@Override
	public Map<String, String> getMonitorInfo() {
		return null;
	}

	@Override
	public void write(ILineReceiver lineReceiver) {}

	@Override
	public void commit() {}

}
