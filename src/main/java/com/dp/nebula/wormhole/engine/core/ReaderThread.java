package com.dp.nebula.wormhole.engine.core;

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.interfaces.IReader;
import com.dp.nebula.wormhole.engine.utils.JarLoader;
import com.dp.nebula.wormhole.engine.utils.ReflectionUtil;

final class ReaderThread implements Callable<Integer>{
	
	private static final Log s_logger = LogFactory.getLog(ReaderThread.class);
	
	private IReader reader;
	private ILineSender lineSender;
	
	public static ReaderThread getInstance(ILineSender lineSender, IParam param, String readerClassName, 
			String readerPath, IPluginMonitor monitor){
		try{
			IReader reader = ReflectionUtil.createInstanceByDefaultConstructor(
					readerClassName, IReader.class, JarLoader.getInstance(readerPath));
			reader.setParam(param);
			reader.setMonitor(monitor);
			return new ReaderThread(lineSender, reader);
		} catch(Exception e){
			s_logger.error("Error to create Reader Thread!", e);
			return null;
		}
	}
	 
	private ReaderThread(ILineSender lineSender, IReader reader){
		super();
		this.lineSender = lineSender;
		this.reader = reader;
	}
	
	

	@Override
	/**
	 * invoke method init, connection, read & finish sequentially
	 * any exception occurs is thrown to upper classes (Thread Pool)
	 */
	public Integer call() throws Exception {
		try{
			reader.init();
			reader.connection();
			reader.read(lineSender);
			reader.finish();
			return JobStatus.SUCCESS.getStatus();
		} catch(WormholeException e){
			s_logger.error("Exception occurs in reader thread!", e);
			return e.getStatusCode();
		} catch(Exception e){
			s_logger.error("Exception occurs in reader thread!", e);
			return JobStatus.FAILED.getStatus();
		}
	}

}
