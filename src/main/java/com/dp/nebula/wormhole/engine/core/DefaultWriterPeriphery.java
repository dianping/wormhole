package com.dp.nebula.wormhole.engine.core;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;

class DefaultWriterPeriphery implements IWriterPeriphery {

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		// do nothing
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter) {
		// do nothing
		
	}

	@Override
	public void rollback(IParam param) {
		// do nothing
	}

}
