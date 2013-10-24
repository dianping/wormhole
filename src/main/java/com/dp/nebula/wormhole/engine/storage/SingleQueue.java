package com.dp.nebula.wormhole.engine.storage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.dp.nebula.wormhole.common.interfaces.ILine;

public class SingleQueue  extends StorageQueue{

	private static final long serialVersionUID = 7014983687729761230L;

	private int lineLimit;

	//the real double queues
	private final ILine[] items;
	
	private ReentrantLock lock;
		
	private Condition notFull;
	
	private Condition notEmpty;

	private volatile int count;
		
	private int  writePosition = 0, readPosition = 0;
		
	private boolean closed = false;
	
	private long lineRx = 0;
	
	private long lineTx = 0;
	
	private long byteRx = 0;

	public String info() {
		return lineRx + ":" + lineTx;
	}

	public SingleQueue(int lineLimit, int byteLimit) {
		if (lineLimit <= 0 || byteLimit <= 0) {
			throw new IllegalArgumentException(
					"Queue initial capacity can't less than 0!");
		}
		this.lineLimit = lineLimit;
		items = new ILine[lineLimit];

		lock = new ReentrantLock();

		notFull = lock.newCondition();
		notEmpty = lock.newCondition();
	}


	public int getLineLimit() {
		return lineLimit;
	}

	public void setILineLimit(int capacity) {
		this.lineLimit = capacity;
	}

	private void insert(ILine line) {
		items[writePosition] = line;
		writePosition = (writePosition + 1) % lineLimit;
		++count;
		++lineRx;
		byteRx += line.length();
	}

	private void insert(ILine[] lines, int size) {
		for (int i = 0; i < size; ++i) {
			items[writePosition] = lines[i];
			writePosition = (writePosition + 1) % lineLimit;
			++count;
			++lineRx;
			byteRx += lines[i].length();
		}
	}

	private ILine extract() {
		ILine e = items[readPosition];
		items[readPosition] = null;
		readPosition = (readPosition + 1) % lineLimit;
		--count;
		++lineTx;
		return e;
	}

	private int extract(ILine[] ea) {
		int readsize = Math.min(ea.length, count);
		for (int i = 0; i < readsize; ++i) {
			ea[i] = items[readPosition];
			items[readPosition] = null;
			readPosition = (readPosition + 1) % lineLimit;
			--count;
			++lineTx;
		}
		return readsize;
	}

	public boolean push(ILine line, long timeout, TimeUnit unit)
			throws InterruptedException {
		if (line == null) {
			throw new NullPointerException();
		}
		long nanoTime = unit.toNanos(timeout);
		lock.lockInterruptibly();
		try {
			for (;;) {
				if (count < lineLimit) {
					insert(line);
					if (count == 1) {
						notEmpty.signal();
					}
					return true;
				}

				// Time out
				if (nanoTime <= 0) {
					return false;
				}
				// keep waiting
				try {
					nanoTime = notFull.awaitNanos(nanoTime);
				} catch (InterruptedException ie) {
					notFull.signal();
					throw ie;
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public boolean push(ILine[] lines, int size, long timeout, TimeUnit unit)
			throws InterruptedException {
		if (lines == null) {
			throw new NullPointerException();
		}
		long nanoTime = unit.toNanos(timeout);
		lock.lockInterruptibly();
		try {
			for (;;) {
				if (count + size <= items.length) {
					insert(lines, size);
					notEmpty.signal();
					return true;
				}

				// Time out
				if (nanoTime <= 0) {
					return false;
				}
				// keep waiting
				try {
					nanoTime = notFull.awaitNanos(nanoTime);
				} catch (InterruptedException ie) {
					notFull.signal();
					throw ie;
				}
			}
		} finally {
			lock.unlock();
		}
	}

	public void close() {
		closed = true;
	}

	public ILine pull(long timeout, TimeUnit unit) throws InterruptedException {
		if(closed){
			return null;
		}
		lock.lockInterruptibly();
		try {
			for (;;) {
				if (count > 0) {
					ILine result = extract();
					notFull.signal();
					return result;
				}
				if (timeout <= 0) {
					return null;
				}
				notEmpty.await(timeout, unit);
			}
		} finally {
			lock.unlock();
		}
	}

	public int pull(ILine[] ea, long timeout, TimeUnit unit)
			throws InterruptedException {
		if(closed){
			return -1;
		}
		lock.lockInterruptibly();
		try {
			for (;;) {
				if (count > 0) {
					int result = extract(ea);
					notFull.signal();
					return result;
				}
				if (timeout <= 0) {
					return 0;
				}
				notEmpty.await(timeout, unit);
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int size() {
		return count;
	}

//	public static void main(ILine []args){
//		ReentrantLock readLock = new ReentrantLock();
//		Condition cond = readLock.newCondition();
//		try {
//			readLock.lock();
//			cond.awaitNanos(-10000000000L);
//			readLock.unlock();
//
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		System.out.println("end");
//	}
}
