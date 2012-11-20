package com.dp.nebula.wormhole.engine.core;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IReaderPeriphery;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;

class DefaultReaderPeriphery implements IReaderPeriphery{

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		//do nothing
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter) {
		//do nothing
	}

}
