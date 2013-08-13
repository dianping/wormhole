package com.dp.nebula.wormhole.engine.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.LineExchangerFactory;
import com.dp.nebula.wormhole.common.config.JobPluginConf;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.interfaces.ISplitter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;
import com.dp.nebula.wormhole.engine.config.PluginConfParamKey;
import com.dp.nebula.wormhole.engine.monitor.MonitorManager;
import com.dp.nebula.wormhole.engine.storage.StorageManager;
import com.dp.nebula.wormhole.engine.utils.JarLoader;
import com.dp.nebula.wormhole.engine.utils.ReflectionUtil;

final class WriterManager extends AbstractPluginManager {

	private static final Log s_logger = LogFactory.getLog(WriterManager.class);
	private static final String JOB_PARAM_FAILED_LINES_THRESHOLD = "failedLinesThreshold";
	private static final int TIME_OUT = 10 * 60 * 60;// 60 * ten minutes

	private Map<String, ExecutorService> writerPoolMap;
	private StorageManager storageManager;
	private MonitorManager monitorManager;
	private Map<String, IWriterPeriphery> writerPeripheryMap;
	private Map<String, IParam> writerToJobParamsMap;
	private Map<String, List<Future<Integer>>> writerToResultsMap;

	private static WriterManager wmInstance;

	private WriterManager(StorageManager storageManager,
			MonitorManager monitorManager, int writerNum) {
		super();
		writerPoolMap = new HashMap<String, ExecutorService>(writerNum);
		writerPeripheryMap = new HashMap<String, IWriterPeriphery>(writerNum);
		writerToJobParamsMap = new HashMap<String, IParam>(writerNum);
		writerToResultsMap = new HashMap<String, List<Future<Integer>>>(
				writerNum);
		this.storageManager = storageManager;
		this.monitorManager = monitorManager;
	}

	public static WriterManager getInstance(StorageManager storageManager,
			MonitorManager monitorManager, int writerNum) {
		if (wmInstance == null) {
			wmInstance = new WriterManager(storageManager, monitorManager,
					writerNum);
		}

		return wmInstance;
	}

	public int getFailedLinesThreshold(String writerID) {
		IParam jobParam = writerToJobParamsMap.get(writerID);
		if (jobParam == null) {
			return 0;
		}
		return jobParam.getIntValue(JOB_PARAM_FAILED_LINES_THRESHOLD, 0);
	}

	public void run(List<JobPluginConf> jobPluginList,
			Map<String, IParam> pluginParamsMap) throws TimeoutException,
			ExecutionException, InterruptedException {
		for (JobPluginConf jobPluginConf : jobPluginList) {

			String writerID = jobPluginConf.getId();
			IParam jobParams = jobPluginConf.getPluginParam();
			writerToJobParamsMap.put(writerID, jobParams);
			IParam pluginParams = pluginParamsMap.get(jobPluginConf
					.getPluginName());
			jobParams.putValue(AbstractPlugin.PLUGINID, writerID);
			String writerPath = pluginParams.getValue(PluginConfParamKey.PATH);
			String writerPeripheryClassName = pluginParams
					.getValue(PluginConfParamKey.PERIPHERY_CLASS_NAME);
			IWriterPeriphery writerPeriphery = null;
			if (StringUtils.isEmpty(writerPeripheryClassName)) {
				writerPeriphery = new DefaultWriterPeriphery();
			} else {
				writerPeriphery = ReflectionUtil
						.createInstanceByDefaultConstructor(
								writerPeripheryClassName,
								IWriterPeriphery.class,
								JarLoader.getInstance(writerPath));
			}
			writerPeripheryMap.put(writerID, writerPeriphery);

			String splitterClassName = pluginParams
					.getValue(PluginConfParamKey.SPLITTER_CLASS_NAME);
			ISplitter splitter = null;
			if (StringUtils.isEmpty(splitterClassName)) {
				splitter = new DefaultSplitter();
			} else {
				splitter = ReflectionUtil.createInstanceByDefaultConstructor(
						splitterClassName, ISplitter.class,
						JarLoader.getInstance(writerPath));
			}

			WritePrepareCallable<List<IParam>> writerCallable = new WritePrepareCallable<List<IParam>>();
			writerCallable.writerPeriphery = writerPeriphery;
			writerCallable.jobParams = jobParams;
			runWithTimeout(new FutureTask<List<IParam>>(writerCallable));
			splitter.init(jobParams);

			WriteSplitCallable<List<IParam>> splitCallable = new WriteSplitCallable<List<IParam>>();
			splitCallable.splitter = splitter;
			List<IParam> splittedParam = (List<IParam>) runWithTimeout(new FutureTask<List<IParam>>(
					splitCallable));

			int concurrency = getConcurrency(jobParams, pluginParams);
			String writeClassName = pluginParams
					.getValue(PluginConfParamKey.PLUGIN_CLASS_NAME);
			ExecutorService writerPool = createThreadPool(concurrency);
			writerPoolMap.put(writerID, writerPool);

			List<Future<Integer>> resultList = new ArrayList<Future<Integer>>();
			for (IParam p : splittedParam) {
				IPluginMonitor pm = monitorManager.newWriterMonitor(writerID);
				WriterThread rt = WriterThread.getInstance(LineExchangerFactory
						.createNewLineReceiver(
								storageManager.getStorageForWriter(writerID),
								null), p, writeClassName, writerPath, pm);

				Future<Integer> r = writerPool.submit(rt);
				resultList.add(r);
			}
			writerToResultsMap.put(writerID, resultList);
			s_logger.info("Writer: " + writerID + " start to write data");
			// Do not accept any new threads
			writerPool.shutdown();
		}
	}

	public void killAll() {
		for (String writerID : writerPoolMap.keySet()) {
			ExecutorService es = writerPoolMap.get(writerID);
			es.shutdownNow();
		}
	}

	class WritePrepareCallable<V> implements Callable<V> {
		IWriterPeriphery writerPeriphery;
		IParam jobParams;

		@Override
		public V call() throws Exception {
			writerPeriphery.prepare(jobParams, monitorManager);
			return null;
		}
	}

	class WriteSplitCallable<V> implements Callable<V> {
		ISplitter splitter;

		@SuppressWarnings("unchecked")
		@Override
		public V call() throws Exception {
			List<IParam> splittedParam = splitter.split();
			return (V) splittedParam;

		}
	}

	public static List<IParam> runWithTimeout(FutureTask<List<IParam>> task)
			throws TimeoutException, ExecutionException, InterruptedException {
		task.run();
		return task.get(TIME_OUT, TimeUnit.SECONDS);
	}

	public boolean terminate(boolean writerConsistency) {
		boolean result = true;
		Set<String> failedIDs = getFailedWriterID();
		if (writerConsistency && failedIDs.size() > 0) {
			for (String writerID : writerPoolMap.keySet()) {
				ExecutorService es = writerPoolMap.get(writerID);
				es.shutdownNow();
				terminate(writerID);
			}
			writerPoolMap.clear();
			return true;
		}
		Collection<String> needToRemoveWriterIDList = new ArrayList<String>(
				writerPoolMap.size());
		for (String writerID : writerPoolMap.keySet()) {
			ExecutorService es = writerPoolMap.get(writerID);
			if (es.isTerminated()) {
				terminate(writerID);
				needToRemoveWriterIDList.add(writerID);
			} else {
				result = false;
			}
		}

		// remove terminated writers from writerPoolMap
		for (String writerID : needToRemoveWriterIDList) {
			writerPoolMap.remove(writerID);
		}

		return result;
	}

	public Set<String> getFailedWriterID() {
		Set<String> failedIDs = new HashSet<String>();
		for (String writerID : writerToResultsMap.keySet()) {
			if (!isSuccess(writerToResultsMap.get(writerID),
					writerPoolMap.get(writerID))) {
				failedIDs.add(writerID);
			}
		}
		return failedIDs;
	}

	public int getErrorCode() {
		int result = Integer.MAX_VALUE;
		for (String writerID : writerToResultsMap.keySet()) {
			int status = getStatus(writerToResultsMap.get(writerID),
					writerPoolMap.get(writerID));
			if (status != JobStatus.SUCCESS.getStatus() && status < result) {
				result = status;
			}
		}
		return result;
	}

	private void terminate(String writerID) {
		IWriterPeriphery writerPeriphery = writerPeripheryMap.get(writerID);
		if (writerPeriphery == null) {
			s_logger.error("can not find any writer periphery for " + writerID);
			return;
		}
		IParam jobParams = writerToJobParamsMap.get(writerID);
		if (jobParams == null) {
			s_logger.error("can not find any job parameters for " + writerID);
			return;
		}

		writerPeriphery.doPost(jobParams, monitorManager);
	}

	public void rollbackAll() {
		for (String writerID : writerPeripheryMap.keySet()) {
			rollback(writerID);
		}
	}

	public void rollback(Collection<String> writerIDs) {
		for (String writerID : writerIDs) {
			rollback(writerID);
		}
	}

	public void rollback(String writerID) {
		IWriterPeriphery writerPeriphery = writerPeripheryMap.get(writerID);
		if (writerPeriphery == null) {
			s_logger.error("can not find any writer periphery for " + writerID);
			return;
		}
		IParam jobParams = writerToJobParamsMap.get(writerID);
		if (jobParams == null) {
			s_logger.error("can not find any job parameters for " + writerID);
			return;
		}

		writerPeriphery.rollback(jobParams);
	}
}
