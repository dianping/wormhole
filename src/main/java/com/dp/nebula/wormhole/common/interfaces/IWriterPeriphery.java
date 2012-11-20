package com.dp.nebula.wormhole.common.interfaces;

public interface IWriterPeriphery extends ITransmissionPeriphery{
	
	void rollback(IParam param);

}
