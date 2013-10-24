package com.dp.nebula.wormhole.engine.core;

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.engine.utils.JarLoader;
import com.dp.nebula.wormhole.engine.utils.ReflectionUtil;

final class WriterThread implements Callable<Integer>{
	
	private static final Log s_logger = LogFactory.getLog(WriterThread.class);

	private IWriter writer;
	private ILineReceiver lineReceiver;
	
	public static WriterThread getInstance(ILineReceiver lineReceiver, IParam param, String writerClassName, 
			String writerPath, IPluginMonitor pm){
		try{
			IWriter writer = ReflectionUtil.createInstanceByDefaultConstructor(
					writerClassName, IWriter.class,
					JarLoader.getInstance(new String[]{writerPath}));
			writer.setParam(param);
			writer.setMonitor(pm);
			return new WriterThread(lineReceiver, writer);
		} catch(Exception e){
			s_logger.error("Error to create WriterThread: ", e);
			return null;
		}
		
	}

	private WriterThread(ILineReceiver lineReceiver, IWriter writer) {
		super();
		this.lineReceiver = lineReceiver;
		this.writer = writer;
	}

	@Override
	public Integer call() throws Exception {
		try{
			writer.init();
			writer.connection();
			writer.write(lineReceiver);
			writer.commit();
			writer.finish();
			return JobStatus.SUCCESS.getStatus();
		} catch(WormholeException e){
			s_logger.error("Exception occurs in writer thread!", e);
			return e.getStatusCode();
		} catch(Exception e){
			s_logger.error("Exception occurs in writer thread!", e);
			return JobStatus.FAILED.getStatus();
		}
	}

}
