package com.dp.nebula.wormhole.common.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dp.nebula.wormhole.common.config.JobConf;
import com.dp.nebula.wormhole.common.config.JobPluginConf;
import com.dp.nebula.wormhole.common.interfaces.IParam;

public class ParseXMLUtilTest {

	@Test
	public void testLoadJobConf() {
		String fileName = "src/test/resources/wormhole_hivereader_to_hdfswriter_test.xml";
		JobConf jobConf = ParseXMLUtil.loadJobConf(fileName);
		assertNotNull(jobConf);
		assertEquals("hivereader_to_hdfswriter_job", jobConf.getId());
		
		JobPluginConf readerConf =  jobConf.getReaderConf();
		List<JobPluginConf> writerConf = jobConf.getWriterConfs();
		
		assertEquals("hivereader", readerConf.getPluginName());
		IParam readerPluginParam = readerConf.getPluginParam();
		assertNotNull(readerPluginParam);
		assertTrue(readerPluginParam instanceof IParam);
		
		assertNotNull(writerConf);
		assertEquals(1, writerConf.size());
		assertEquals("hdfswriter", writerConf.get(0).getPluginName());
	}

	@Test
	public void testLoadPluginConf() {
		Map<String, IParam> pluginMap = ParseXMLUtil.loadPluginConf();
		assertNotNull(pluginMap); 
		Iterator iter = pluginMap.entrySet().iterator(); 
		while (iter.hasNext()) { 
		    Map.Entry entry = (Map.Entry) iter.next(); 
		    System.out.println(entry.getKey());
		    System.out.println(entry.getValue());
		} 
	}

	@Test
	public void testLoadEngineConfig() {
		IParam engineConf = ParseXMLUtil.loadEngineConfig();
		assertNotNull(engineConf);
		assertNotNull(engineConf.getValue("storageClassName"));
	}

}
