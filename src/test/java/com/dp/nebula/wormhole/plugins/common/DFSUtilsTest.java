package com.dp.nebula.wormhole.plugins.common;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DFSUtilsTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testGetTypeMap() throws IOException{
		assertNotNull(DFSUtils.getTypeMap());
		assertNotNull(DFSUtils.getCompressionSuffixMap());
	}
	
	@Test
	public void testGetConfiguration() throws Exception{
		DFSUtils.getConf("file:///data/home/workcron/imglog/", "");
//		Assert
//		DFSUtils.getConf("/data/home/workcron/imglog/", "");
//		Assert.fail("get conf failed");
	}

}
