package com.dp.nebula.wormhole.common;

import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ITransformer;

public abstract class AbstractTransformer implements ITransformer{

	@Override
	public ILine transform(ILine line, String params) {
		if(params == null || params.equals(""))
			return transform(line);
		else 
			return line;
	}

	@Override
	public ILine transform(ILine line) {
		return line;
	}
	
}
