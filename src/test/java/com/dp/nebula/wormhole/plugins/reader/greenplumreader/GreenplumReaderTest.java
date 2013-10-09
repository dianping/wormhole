package com.dp.nebula.wormhole.plugins.reader.greenplumreader;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.dp.nebula.wormhole.common.BufferedLineExchanger;
import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.utils.ParseXMLUtil;
import com.dp.nebula.wormhole.engine.config.EngineConfParamKey;
import com.dp.nebula.wormhole.engine.storage.StorageConf;
import com.dp.nebula.wormhole.engine.storage.StorageManager;
import com.dp.nebula.wormhole.plugins.reader.greenplumreader.ParamKey;

public class GreenplumReaderTest {
	
	public static BufferedLineExchanger getLineExchanger(){
		IParam engineConf = null;
		engineConf = ParseXMLUtil.loadEngineConfig();
		List<StorageConf> result = new ArrayList<StorageConf>();
		
		for(int i = 0; i< 1; i++){
			StorageConf storageConf = new StorageConf();
			storageConf.setId(String.valueOf(i));
			storageConf.setStorageClassName(
					engineConf.getValue(EngineConfParamKey.STORAGE_CLASS_NAME));
			storageConf.setLineLimit(
					10);
			storageConf.setByteLimit(
					engineConf.getIntValue(EngineConfParamKey.STORAGE_BYTE_LIMIT));
			storageConf.setDestructLimit(
					engineConf.getIntValue(EngineConfParamKey.STORAGE_DESTRUCT_LIMIT));
			storageConf.setPeriod(
					engineConf.getIntValue(EngineConfParamKey.MONITOR_INFO_DISPLAY_PERIOD));
			storageConf.setWaitTime(
					5000
					
			);
			result.add(storageConf);
		}
		StorageManager manager = new StorageManager(result);
		return new BufferedLineExchanger(manager.getStorageForWriter("0"), manager.getStorageForReader());
	}
	//@Test
	public void testGPReader() {
		GreenplumReaderPeriphery gpPeriphery = new GreenplumReaderPeriphery();
		GreenplumReader gpReader = new GreenplumReader();
		
		Map<String, String> params = new HashMap<String, String>();
		
		params.put(ParamKey.ip, "10.1.21.57");
		params.put(ParamKey.port, "5432");
		params.put(ParamKey.dbname, "dianpingdw");
		params.put(ParamKey.username, "tempuser_201302211841");
		params.put(ParamKey.password,"dp!@g0Y8bRfvL");

		params.put(ParamKey.sql, "copy (select * from dpmid.mid_dp_shop_info_his where cal_dt = '2012-01-01' limit 20) to stdout WITH DELIMITER E'\t' ");
		IParam iParam = new DefaultParam(params);
		
		gpPeriphery.prepare(iParam,null);
		gpReader.setParam(iParam);
		BufferedLineExchanger bufLineExchanger = getLineExchanger();
		
		IPluginMonitor pluginMonitor = mock(IPluginMonitor.class);
		gpReader.setMonitor(pluginMonitor);
		
		gpReader.init();
		gpReader.connection();
		ReadRunnable t = new ReadRunnable();
		t.init(gpReader,bufLineExchanger);
		new Thread(t).start();
		ILine line = null;
		System.out.println("start");
		int num = 0;
		try{
			while((line = bufLineExchanger.receive())!=null) {
				num++;
				if(line.getFieldNum() != 14){
					for(int i=0; i <line.getFieldNum(); i++){
						System.out.print(line.getField(i)+"\t");
					}
					System.out.println(line.getFieldNum());
				}

			}
		} catch (Exception e){
		}
		System.out.println("end, transfer " + num);	
		
	}
	@Test
	public void testPgStringNormalize() {
		GreenplumReader gpReader = new GreenplumReader();
		assertEquals("df\r\t\\",gpReader.pgStringNormalize("df\\r\\t\\\\"));
		
	}
	private class ReadRunnable implements Runnable{
		GreenplumReader gpReader;
		BufferedLineExchanger bufLineExchanger;
		@Override
		public void run() {
			gpReader.read(bufLineExchanger);
			gpReader.finish();
		}
		public void init(GreenplumReader gpReader,BufferedLineExchanger bufLineExchanger){
			this.gpReader = gpReader;
			this.bufLineExchanger = bufLineExchanger;
		}
	};
	
}
