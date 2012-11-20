package com.dp.nebula.wormhole.engine.core;

import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.engine.config.PluginConfParamKey;
import com.dp.nebula.wormhole.plugins.common.ParamKey;

abstract class AbstractPluginManager {
	
	private static final Log s_logger = LogFactory.getLog(AbstractPluginManager.class);
	private static final String PARAM_KEY_CURRENCY = "concurrency";
	private final static String WORMHOLE_CONNECT_FILE = "WORMHOLE_CONNECT_FILE";

	
	/**
	 * get number of reader/writer threads running concurrently
	 * it's determined by parameter concurrency set in job.xml
	 * the valid value is between 1 to MAX_THREAD_NUMBER (it's set per plugin in plugin.xml)
	 * 
	 * @param jobParams
	 * @param pluginParams
	 * @return int 
	 */
	protected int getConcurrency(IParam jobParams, IParam pluginParams){
		int concurrency = jobParams.getIntValue(PARAM_KEY_CURRENCY, 1);
		int maxThreadNum = pluginParams.getIntValue(PluginConfParamKey.MAX_THREAD_NUMBER);
		if(concurrency <=0 || concurrency > maxThreadNum){
			s_logger.info("concurrency in conf:" + concurrency + " is invalid!");
			concurrency = 1;
		}
		
		return concurrency;
	}
	
	/**
	 * create thread pool to run reader/writer threads
	 * 
	 * @param concurrency
	 * @return ExecutorService
	 */
	protected ExecutorService createThreadPool(int concurrency){
		ThreadPoolExecutor tp = new ThreadPoolExecutor(concurrency, concurrency, 1L,TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
		tp.prestartCoreThread();
		return tp;
	}
	
	/**
	 * Decide whether the reader or writer thread is executed successfully
	 * if there is NO thread failed, it returns true; even if some of the thread is running
	 * 
	 * @param threadResultList
	 * @return boolean
	 */
	protected int getStatus(List<Future<Integer>> threadResultList, ExecutorService threadPool){
		for(Future<Integer> r: threadResultList){
			try {
				Integer result = r.get(1, TimeUnit.MICROSECONDS);
				if(result == null || result != JobStatus.SUCCESS.getStatus()){
					if(threadPool != null){
					//if one thread failed, stop all other threads in the thread pool
						threadPool.shutdownNow();
					}
					return result;
				}
			}catch(TimeoutException e){
				s_logger.debug("thread is not finished yet");
				continue;
			}catch (InterruptedException e) {
				s_logger.error("Interrupted Exception occurs when getting thread result!");
				continue;
			} catch (ExecutionException e) {
				threadPool.shutdownNow();
				s_logger.error("Execution Exception occurs when getting thread result, this should never happen!", e);
				return JobStatus.FAILED.getStatus();
			}
		}
		return JobStatus.SUCCESS.getStatus();
	}
	
	public boolean isSuccess(List<Future<Integer>> threadResultList, ExecutorService threadPool) {
		return getStatus(threadResultList,threadPool)==JobStatus.SUCCESS.getStatus();
	}
	
	public static void regDataSourceProp(IParam param) {
		String fileName = System.getenv(WORMHOLE_CONNECT_FILE);
		String connectProps = param.getValue(ParamKey.connectProps,null);
		if (fileName != null && connectProps != null) {
			Properties props = new Properties();
			try {
				props.load(new FileInputStream(fileName));
				param.putValue(ParamKey.ip, props.getProperty(connectProps + "." + ParamKey.ip).trim());
				param.putValue(ParamKey.port, props.getProperty(connectProps + "." + ParamKey.port).trim());
				param.putValue(ParamKey.username, props.getProperty(connectProps + "." + ParamKey.username).trim());
				param.putValue(ParamKey.password, props.getProperty(connectProps + "." + ParamKey.password).trim());
				param.putValue(ParamKey.dbname, props.getProperty(connectProps + "." + ParamKey.dbname).trim());
			} catch (Exception e) {
				s_logger.error(e.getMessage(),e);
				throw new WormholeException(e,JobStatus.CONF_FAILED.getStatus());	
			}
		}
	}
}
