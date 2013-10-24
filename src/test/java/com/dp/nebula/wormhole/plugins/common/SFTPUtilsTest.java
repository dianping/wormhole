package com.dp.nebula.wormhole.plugins.common;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

public class SFTPUtilsTest {

	@Test
	public void testGetConf() {
		Configuration cfg = SFTPUtils.getConf();
		assertNotNull(cfg);
		assertNotNull(cfg.get("io.compression.codecs"));
		System.out.println(cfg.get("io.compression.codecs"));
	}

}
