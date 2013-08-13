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

import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;
import com.dp.nebula.wormhole.common.interfaces.IReader;


public class ReaderThreadTest {
	
	private ILineSender lineSender;
	private IParam param; 
	private IPluginMonitor monitor;
	
	@Before
	public void init(){
		lineSender = mock(ILineSender.class);
		param = mock(IParam.class);
		monitor = mock(IPluginMonitor.class);
	}
	
	@Test
	public void testConstructor_Success(){
		ReaderThread reader = createReaderThread("com.dp.nebula.wormhole.engine.common.FakeReader");
		assertNotNull(reader);
		assertTrue(reader instanceof ReaderThread);
	}
	
	@Test
	public void testConstuctor_Failed(){
		ReaderThread reader = createReaderThread("com.dp.nebula.wormhole.engine.common.NotExistReader");
		assertNull(reader);
	}
	
	@Test
	public void testRun() throws Exception{
		IReader reader = mock(IReader.class);
		ReaderThread readerThread = createReaderThread(reader);
		readerThread.call();
		
		InOrder orderedExecution = inOrder(reader);
		orderedExecution.verify(reader).init();	
		orderedExecution.verify(reader).connection();
		orderedExecution.verify(reader).read(lineSender);
		orderedExecution.verify(reader).finish();
	}
	
	@SuppressWarnings("unchecked")
	private ReaderThread createReaderThread(IReader reader){
		try {
			Class clazz = Class.forName("com.dp.nebula.wormhole.engine.core.ReaderThread");
			Constructor con = clazz.getDeclaredConstructor(ILineSender.class, IReader.class);
			con.setAccessible(true);
			return (ReaderThread)con.newInstance(lineSender, reader);
			
		} catch (Exception e) {
			return null;
		}
	}

	private ReaderThread createReaderThread(String className){
		return ReaderThread.getInstance(lineSender, param, 
				className, null, monitor);
	}
}
