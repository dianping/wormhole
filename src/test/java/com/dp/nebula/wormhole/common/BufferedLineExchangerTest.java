package com.dp.nebula.wormhole.common;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.utils.ParseXMLUtil;
import com.dp.nebula.wormhole.engine.config.EngineConfParamKey;
import com.dp.nebula.wormhole.engine.storage.StorageConf;
import com.dp.nebula.wormhole.engine.storage.StorageManager;

public class BufferedLineExchangerTest {
	public static BufferedLineExchanger getLineExchanger(){
		IParam engineConf = null;
		engineConf = ParseXMLUtil.loadEngineConfig();
		List<StorageConf> result = new ArrayList<StorageConf>();
		
		for(int i = 0; i< 5; i++){
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
					1000);
			result.add(storageConf);
		}
		StorageManager manager = new StorageManager(result);
		return new BufferedLineExchanger(manager.getStorageForWriter("1"), manager.getStorageForReader());
	}
	@Test
	public void init(){
		BufferedLineExchanger exchanger = getLineExchanger();
		ILine line = new DefaultLine();
		line.addField("this");
		line.addField("that");
		exchanger.send(line);
		exchanger.flush();
		ILine getLine = exchanger.receive();
		assertEquals(line,getLine);
	}
}