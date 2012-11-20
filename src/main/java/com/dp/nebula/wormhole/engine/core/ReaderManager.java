package com.dp.nebula.wormhole.engine.core;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.LineExchangerFactory;
import com.dp.nebula.wormhole.common.config.JobPluginConf;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.interfaces.IReaderPeriphery;
import com.dp.nebula.wormhole.common.interfaces.ISplitter;
import com.dp.nebula.wormhole.engine.config.PluginConfParamKey;
import com.dp.nebula.wormhole.engine.monitor.MonitorManager;
import com.dp.nebula.wormhole.engine.storage.StorageManager;
import com.dp.nebula.wormhole.engine.utils.JarLoader;
import com.dp.nebula.wormhole.engine.utils.ReflectionUtil;

final class ReaderManager extends AbstractPluginManager{

	private static final Log s_logger = LogFactory.getLog(ReaderManager.class);

	private ExecutorService readerPool;
	private StorageManager storageManager;
	private MonitorManager monitorManager;
	private IReaderPeriphery readerPeriphery;
	private IParam jobParams;
	private List<Future<Integer>> threadResultList; 

	private static ReaderManager rmInstance;

	private ReaderManager(StorageManager storageManager,
			MonitorManager monitorManager) {
		super();
		this.storageManager = storageManager;
		this.monitorManager = monitorManager;
		threadResultList = new ArrayList<Future<Integer>>();
	}

	public static ReaderManager getInstance(StorageManager storageManager, MonitorManager monitorManager){
		if(rmInstance == null){
			rmInstance = new ReaderManager(storageManager, monitorManager);
		}
		return rmInstance;
	}
	
	/**
	 * this method is response for doing some prepare work, and then create reader threads, 
	 * and submit them to the thread pool 
	 * 
	 * @param jobPluginConf
	 * @param pluginParams
	 */
	public void run(JobPluginConf jobPluginConf, IParam pluginParams){
		//jobParams are the parameters for specific data exchange jobs, they come from jobXXX.xml
		//pluginParams are global settings for certain plugin, they are set in conf/plugins.xml
		
		jobParams = jobPluginConf.getPluginParam();

		String readerPath = pluginParams.getValue(PluginConfParamKey.PATH);
		String readerPeripheryClassName = pluginParams.getValue(PluginConfParamKey.PERIPHERY_CLASS_NAME);
		if(StringUtils.isEmpty(readerPeripheryClassName)){
			readerPeriphery = new DefaultReaderPeriphery();
		}
		else{
			readerPeriphery = ReflectionUtil.createInstanceByDefaultConstructor(
					readerPeripheryClassName, 
					IReaderPeriphery.class,
					JarLoader.getInstance(readerPath));
		}
		readerPeriphery.prepare(jobParams, monitorManager);

		String splitterClassName = pluginParams.getValue(PluginConfParamKey.SPLITTER_CLASS_NAME);
		ISplitter splitter = null;
		if(StringUtils.isEmpty(splitterClassName)){
			splitter = new DefaultSplitter();
		}
		else{
			splitter = ReflectionUtil.createInstanceByDefaultConstructor(
					splitterClassName, 
					ISplitter.class,
					JarLoader.getInstance(readerPath));
		}
		splitter.init(jobParams);
		List<IParam> splittedParam = splitter.split();

		int concurrency = getConcurrency(jobParams, pluginParams);
		String readerClassName = pluginParams.getValue(PluginConfParamKey.PLUGIN_CLASS_NAME);
		readerPool = createThreadPool(concurrency);

		for (IParam p : splittedParam) {
			IPluginMonitor pm = monitorManager.newReaderMonitor();
			ReaderThread rt = ReaderThread.getInstance(
					LineExchangerFactory.createNewLineSender(
							null, storageManager.getStorageForReader()),
							p, readerClassName, readerPath, pm);

			Future<Integer> r = readerPool.submit(rt);
			threadResultList.add(r);
		}
		s_logger.info("Nebula WormHole start to read data");
		//Do not accept any new threads
		readerPool.shutdown();
	}

	public boolean terminate(){
		isSuccess();
		if(readerPool.isTerminated()){
			readerPeriphery.doPost(jobParams, monitorManager);
			return true;
		}
		return false;
	}
	
	public boolean isSuccess(){
		return isSuccess(threadResultList, readerPool);
	}
	
	public int getStatus() {
		return getStatus(threadResultList, readerPool);
	}
}
