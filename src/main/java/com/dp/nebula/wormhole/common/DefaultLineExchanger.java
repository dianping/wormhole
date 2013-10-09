package com.dp.nebula.wormhole.common;

import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.engine.storage.IStorage;

public class DefaultLineExchanger implements ILineSender, ILineReceiver{

	private IStorage storage;

	public DefaultLineExchanger(IStorage storage) {
		super();
		this.storage = storage;
	}

	@Override
	public ILine createNewLine() {
		return new DefaultLine();
	}

	@Override
	public Boolean send(ILine line) {
		return storage.push(line);
	}

	@Override
	public void flush() {		
	}

	@Override
	public ILine receive() {
        return storage.pull();
	}

}
