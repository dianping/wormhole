package com.dp.nebula.wormhole.engine.core;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Test;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.config.JobConf;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.utils.Environment;
import com.dp.nebula.wormhole.common.utils.JobConfGenDriver;
import com.dp.nebula.wormhole.common.utils.ParseXMLUtil;

public class EngineTest {
	private static final Log s_logger = LogFactory.getLog(EngineTest.class);
	@Test
	public void test() {
		String []args = {"job.xml"};
		PropertyConfigurator.configure(Environment.LOG4J_CONF);
		
		String jobDescriptionXML = null;
		//if no parameters are passed in, 
		//it generates job configure XML first
		if(args.length < 1){
			try{
				JobConfGenDriver.generateJobConfXml();
			} catch(Exception e){
				s_logger.error("Error in generating job configure XML: ", e);
				System.exit(JobStatus.FAILED.getStatus());
			}
			System.exit(JobStatus.SUCCESS.getStatus());
		}
		else if(args.length == 1){
			jobDescriptionXML = args[0];
		}
		//return usage information
		else{
			s_logger.error("Usage: ./wormhole.sh job.xml .");
			System.exit(JobStatus.FAILED.getStatus());
		}
		JobConf jobConf = null;
		IParam engineConf = null;
		Map<String, IParam> pluginConfs = null;
		try{
			//read configurations from XML for engine & plugins
			jobConf = ParseXMLUtil.loadJobConf(jobDescriptionXML);
			engineConf = ParseXMLUtil.loadEngineConfig();
			pluginConfs = ParseXMLUtil.loadPluginConf();
			//start data transmission
			
		} catch (Exception e) {
			s_logger.error("Configure file error occurs: ",e);
			System.exit(JobStatus.CONF_FAILED.getStatus());
		}
		
		Engine engine = new Engine(engineConf, pluginConfs);
		int jobStatus = engine.run(jobConf);
		s_logger.info("wormhole return code-" + jobStatus);
		assertEquals(0,jobStatus);
	}
}
