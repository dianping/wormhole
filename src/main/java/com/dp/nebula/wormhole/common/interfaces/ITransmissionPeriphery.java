package com.dp.nebula.wormhole.common.interfaces;


interface ITransmissionPeriphery {
	
	void prepare(IParam param, ISourceCounter counter);
	
	void doPost(IParam param, ITargetCounter counter, int faildSize);

}
