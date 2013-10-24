package com.dp.nebula.wormhole.common;

import java.util.List;

import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.engine.storage.IStorage;

public class LineExchangerFactory {
	
	public static ILineSender createNewLineSender(IStorage storageForRead, List<IStorage> storageForWrite){
	
		return new BufferedLineExchanger(storageForRead, storageForWrite);
	}
	
	public static ILineReceiver createNewLineReceiver(IStorage storageForRead, List<IStorage> storageForWrite){

		return new BufferedLineExchanger(storageForRead, storageForWrite);
	}

}
