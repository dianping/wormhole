package com.dp.nebula.wormhole.engine.storage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.dp.nebula.wormhole.common.DefaultLine;
import com.dp.nebula.wormhole.common.interfaces.ILine;

public class SingleQueueTest {
	@Test
	public void queueTest(){
		SingleQueue dq = new SingleQueue(200,1024*200);
		ILine setLine,getLine = null;
		setLine = new DefaultLine();
		setLine.addField("1");
		setLine.addField("sunny");
		try {
			dq.push(setLine, 1, TimeUnit.MILLISECONDS);
			getLine = dq.pull(1, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			Thread.sleep ( 1000L ) ;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertEquals(setLine,getLine);
	}
	@Test
	public void bufferedQueueTest(){
		SingleQueue dq = new SingleQueue(200,1024*200);
		ILine[] setLines,getLines = null;
		setLines = new ILine[10];
		getLines = new ILine[10];
		ILine item1 = new DefaultLine();
		item1.addField("1");
		item1.addField("sunny");
		ILine item2 = new DefaultLine();
		item2.addField("2");
		item2.addField("jack");
		setLines[0]=item1;
		setLines[1]=item2;
		try {
			dq.push(setLines,2, 1, TimeUnit.MILLISECONDS);
			dq.pull(getLines, 1, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		try {
			Thread.sleep ( 1000L ) ;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		assertArrayEquals(setLines,getLines);
	}
}
