package com.dp.nebula.wormhole.transform.utils;

import static org.junit.Assert.*;


import org.junit.Test;

public class FieldTransformUtilsTest {
	
	@Test
	public void testFromUnixTime() {
		assertEquals("2012-07-18 00:00:00",FieldTransformUtils.fromUnixTime(1342540800L));
	}
}
