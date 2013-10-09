package com.dp.nebula.wormhole.plugins.writer.hbasewriter;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.google.common.base.Preconditions;

public class HBaseSplitter extends AbstractSplitter {
	private final static Logger LOG = Logger.getLogger(HBaseSplitter.class);

	@Override
	public List<IParam> split() {
		List<IParam> result = new ArrayList<IParam>();
		int concurrency = param.getIntValue(ParamKey.concurrency, 1);
		Preconditions.checkArgument((concurrency > 0 && concurrency <= 10),
				"illegal concurrency number argument " + concurrency);
		
		for (int i = 0; i < concurrency; i++) {
			IParam iParam = param.clone();
			result.add(iParam);
		}
		LOG.info("the number of split: " + result.size());
		return result;
	}
}
