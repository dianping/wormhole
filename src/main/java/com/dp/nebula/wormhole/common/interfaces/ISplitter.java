package com.dp.nebula.wormhole.common.interfaces;

import java.util.List;

public interface ISplitter {
	
	void init(IParam jobParams);
	
	List<IParam> split();

}
