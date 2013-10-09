package com.dp.nebula.wormhole.common;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;


import com.dp.nebula.wormhole.common.interfaces.IParam;

public class DefaultParamTest { 
	
	private IParam param;

	@Before
    public void init() {
		Map<String, String> map = new HashMap<String, String> ();
		param = new DefaultParam(map);
		map.put("booltrue", "true");
		map.put("boolfalse", "error");
		map.put("chart", "\\t");
		map.put("char1", "\001");
		map.put("char2", "\\u0051");
		map.put("char3", "\\001");
		map.put("arrayList", "1:2");
		map.put("int", "31");
		map.put("long", "31");
		map.put("double", "31.13");
		map.put("string", "system");

	}
	
	@Test
	public void test() {
		assertEquals(true,param.getBooleanValue("booltrue"));
		assertEquals(false,param.getBooleanValue("boolfalse",false));
		assertEquals('\t',param.getCharValue("chart"));
		assertEquals('\001',param.getCharValue("char1"));
		assertEquals('3',param.getCharValue("char2"));
		assertEquals('\001',param.getCharValue("char3"));

		assertEquals(31,param.getIntValue("int"));
		assertEquals(31L,param.getLongValue("long"));
		assertEquals((double)31.13,param.getDoubleValue("double"),0.1);

		List<Character> result = new ArrayList<Character>();
		result.add('1');
		result.add('2');
		assertEquals(result,param.getCharList("arrayList"));
		assertEquals("system", param.getValue("string","see"));
		
	}

}
