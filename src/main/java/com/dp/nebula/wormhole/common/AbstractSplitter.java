package com.dp.nebula.wormhole.common;

import java.util.ArrayList;
import java.util.List;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISplitter;

public abstract class AbstractSplitter implements ISplitter{
	
	protected IParam param;
	
	@Override
	public void init(IParam jobParams){
		param = jobParams;
	}
	
	@Override
	public List<IParam> split(){
		List<IParam> result = new ArrayList<IParam>();
		result.add(param);
		return result;
	}


}
