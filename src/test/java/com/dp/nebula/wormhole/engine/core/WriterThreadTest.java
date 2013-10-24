package com.dp.nebula.wormhole.engine.core;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

import java.lang.reflect.Constructor;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.interfaces.IWriter;

public class WriterThreadTest {
	
	private ILineReceiver lineReceiver;
	private IParam param; 
	private IPluginMonitor monitor;
	
	@Before
	public void init(){
		lineReceiver = mock(ILineReceiver.class);
		param = mock(IParam.class);
		monitor = mock(IPluginMonitor.class);
	}
	
	@Test
	public void testConstructor_Success(){
		WriterThread writer = createWriterThread("com.dp.nebula.wormhole.engine.common.FakeWriter");
		assertNotNull(writer);
		assertTrue(writer instanceof WriterThread);
	}
	
	@Test
	public void testConstuctor_Failed(){
		WriterThread writer = createWriterThread("com.dp.nebula.wormhole.engine.common.NotExistWriter");
		assertNull(writer);
	}
	
	@Test
	public void testRun() throws Exception{
		IWriter writer = mock(IWriter.class);
		WriterThread writerThread = createWriterThread(writer);
		writerThread.call();
		
		InOrder orderedExecution = inOrder(writer);
		orderedExecution.verify(writer).init();	
		orderedExecution.verify(writer).connection();
		orderedExecution.verify(writer).write(lineReceiver);
		orderedExecution.verify(writer).commit();
		orderedExecution.verify(writer).finish();
	}
	
	@SuppressWarnings("unchecked")
	private WriterThread createWriterThread(IWriter writer){
		try {
			Class clazz = Class.forName("com.dp.nebula.wormhole.engine.core.WriterThread");
			Constructor con = clazz.getDeclaredConstructor(ILineReceiver.class, IWriter.class);
			con.setAccessible(true);
			return (WriterThread)con.newInstance(lineReceiver, writer);
		} catch (Exception e) {
			return null;
		}
	}

	private WriterThread createWriterThread(String className){
		return WriterThread.getInstance(lineReceiver, param, 
				className, null, monitor);
	}
}
