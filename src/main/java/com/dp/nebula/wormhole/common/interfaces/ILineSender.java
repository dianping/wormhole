package com.dp.nebula.wormhole.common.interfaces;

public interface ILineSender {
	
	ILine createNewLine();
	
	Boolean send(ILine line);
	
	void flush();

}
