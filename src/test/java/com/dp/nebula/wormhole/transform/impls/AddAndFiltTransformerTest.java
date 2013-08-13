package com.dp.nebula.wormhole.transform.impls;


import org.junit.Test;

import com.dp.nebula.wormhole.common.DefaultLine;
import com.dp.nebula.wormhole.common.interfaces.ILine;

public class AddAndFiltTransformerTest {
	@Test
	public void testTransform(){
		ILine line = new DefaultLine();
		line.addField("first");
		line.addField("second");
		line.addField("third");
		
		ILine result = new DefaultLine();
		result.addField("third");
		result.addField("second");
		result.addField("1,2");
		result.addField("first");

		
		result = new DefaultLine();
		result.addField("good");
	}
}
