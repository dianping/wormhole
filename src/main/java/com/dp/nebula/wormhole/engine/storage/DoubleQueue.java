package com.dp.nebula.wormhole.engine.storage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.dp.nebula.wormhole.common.interfaces.ILine;

public class DoubleQueue extends StorageQueue
{
	private static final long serialVersionUID = -7821124845002857361L;

	private int lineLimit;
	
	//private int byteLimit;

	//the real double queues
	private final ILine[] itemsA;
	
	private final ILine[] itemsB;

	/**
	 * writeArray : in reader's eyes, reader get data from data source and write data to this line array.
	 * readArray : in writer's eyes, writer put data to data destination from this line array.
	 * readCount and writeCount are counters of these two array
	 * readPosition and writePosition is the position now been read or wrote
	 * 
	 * Because of the doubleQueue mechanism, the two line will exchange when time is suitable.
	 * 
	 */
	private ILine[] writeArray, readArray;
	
	private volatile int writeCount, readCount;
	
	private int  writePosition, readPosition;

	private ReentrantLock readLock, writeLock;
		
	private Condition notFull;
	
	private Condition awake;
	
	private boolean closed = false;
	
	private int spillSize = 0;

	private long lineRx = 0;
	
	private long lineTx = 0;
	
	/**	received byte number of data from data source(eg:httpreader load data from httpurl) */
	private long byteRx = 0;

	/**
	 * Get info of line number in {@link DoubleQueue} space. 
	 * 
	 * @return
	 * 			Information of line number.
	 * 
	 */
	public String info() {
		return "Read " + lineRx + " | Write " + lineTx + " |";
	}

	/**
	 * Use the two parameters to construct a {@link DoubleQueue} which hold the swap areas.
	 * 
	 * @param	lineLimit
	 * 			Limit of the line number the {@link DoubleQueue} can hold.
	 * 
	 * @param	byteLimit
	 * 			Limit of the bytes the {@link DoubleQueue} can hold.
	 * 
	 */
	public DoubleQueue(int lineLimit, int byteLimit) {
		if (lineLimit <= 0 || byteLimit <= 0) {
			throw new IllegalArgumentException(
					"Queue initial capacity can't less than 0!");
		}
		this.lineLimit = lineLimit;
//		this.byteLimit = byteLimit;
		itemsA = new ILine[lineLimit];
		itemsB = new ILine[lineLimit];

		readLock = new ReentrantLock();
		writeLock = new ReentrantLock();

		notFull = writeLock.newCondition();
		awake = writeLock.newCondition();

		readArray = itemsA;
		writeArray = itemsB;
		spillSize = lineLimit * 8 / 10;
	}

	/**
	 * Get line number of the {@link DoubleQueue}
	 * 
	 * @return	lineLimit 
	 * 			Limit of the line number the {@link DoubleQueue} can hold.
	 * 
	 */
	public int getLineLimit() {
		return lineLimit;
	}

	/**
	 * Set line number of the {@link DoubleQueue}.
	 * 
	 * @param	capacity
	 * 			Limit of the line number the {@link DoubleQueue} can hold.
	 * 
	 */
	public void setLineLimit(int capacity) {
		this.lineLimit = capacity;
	}

	/**
	 * Insert one line of record to a apace which buffers the swap data.
	 * 
	 * @param	line
	 * 			The inserted line.
	 * 
	 */
	private void insert(ILine line) {
		writeArray[writePosition] = line;
		++writePosition;
		++writeCount;
		++lineRx;
		byteRx += line.length();
	}

	/**
	 * Insert a line array(appointed the limit of array size) of data to a apace which buffers the swap data.
	 * 
	 * @param lines
	 * 			Inserted line array.
	 * 
	 * @param size
	 * 			Limit of inserted size of the line array.
	 * 
	 */
	private void insert(ILine[] lines, int size) {
		for (int i = 0; i < size; ++i) {
			writeArray[writePosition] = lines[i];
			++writePosition;
			++writeCount;
			++lineRx;
			byteRx += lines[i].length();
		}
	}

	/**
	 * Extract one line of record from the space which contains current data.
	 * 
	 * @return	line
	 * 			A line of data.
	 * 
	 */
	private ILine extract() {
		ILine e = readArray[readPosition];
		readArray[readPosition] = null;
		++readPosition;
		--readCount;
		++lineTx;
		return e;
	}


	/**
	 * Extract a line array of data from the space which contains current data.
	 * 
	 * @param ea
     * @return
	 * 			Extracted line number of data.
	 * 
	 */
	private int extract(ILine[] ea) {
		int readsize = Math.min(ea.length, readCount);
		for (int i = 0; i < readsize; ++i) {
			ea[i] = readArray[readPosition];
			readArray[readPosition] = null;
			++readPosition;
			--readCount;
			++lineTx;
		}
		return readsize;
	}

	/**
	 * switch condition: read queue is empty && write queue is not empty.
	 * Notice:This function can only be invoked after readLock is grabbed,or may
	 * cause dead lock.
	 * 
	 * @param	timeout
	 * 
	 * @param	isInfinite
	 *          whether need to wait forever until some other thread awake it.
	 *          
	 * @return
	 * 
	 * @throws InterruptedException
	 * 
	 */

	private long queueSwitch(long timeout, boolean isInfinite)
			throws InterruptedException {
		writeLock.lock();
		try {
			if (writeCount <= 0) {
				if (closed) {
					return -2;
				}
				try {
					if (isInfinite && timeout <= 0) {
						awake.await();
						return -1;
					} else {
						return awake.awaitNanos(timeout);
					}
				} catch (InterruptedException ie) {
					awake.signal();
					throw ie;
				}
			} else {
				ILine[] tmpArray = readArray;
				readArray = writeArray;
				writeArray = tmpArray;

				readCount = writeCount;
				readPosition = 0;

				writeCount = 0;
				writePosition = 0;

				notFull.signal();
				// logger.debug("Queue switch successfully!");
				return -1;
			}
		} finally {
			writeLock.unlock();
		}
	}

	
	/**
	 * If exists write space, it will return true, and write one line to the space.
	 * otherwise, it will try to do that in a appointed time,when time is out if still failed, return false. 
	 * 
	 * @param	line
	 * 			a Line.
	 * 
	 * @param	timeout
	 * 			appointed limit time
	 * 
	 * @param	unit
	 * 			time unit
	 * 
	 * @return
	 * 			True if success,False if failed.
	 * 
	 */
	public boolean push(ILine line, long timeout, TimeUnit unit)
			throws InterruptedException {
		if (line == null) {
			throw new NullPointerException();
		}
		long nanoTime = unit.toNanos(timeout);
		writeLock.lockInterruptibly();
		try {
			for (;;) {
				if (writeCount < writeArray.length) {
					insert(line);
					if (writeCount == 1) {
						awake.signal();
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
			writeLock.unlock();
		}
	}

	/**
	 * If exists write space, it will return true, and write a line array to the space.<br>
	 * otherwise, it will try to do that in a appointed time,when time out if still failed, return false. 
	 * 
	 * @param	lines
	 * 			line array contains lines of data
	 * 
	 * @param	size
	 * 			Line number needs to write to the space.
	 * 
	 * @param	timeout
	 * 			appointed limit time
	 * 
	 * @param	unit
	 * 			time unit
	 * 
	 * @return
	 * 			status of this operation, true or false.
	 * 
	 * @throws	InterruptedException
	 * 			if being interrupted during the try limit time.
	 * 
	 */
	public boolean push(ILine[] lines, int size, long timeout, TimeUnit unit)
			throws InterruptedException {
		if (lines == null) {
			throw new NullPointerException();
		}
		long nanoTime = unit.toNanos(timeout);
		writeLock.lockInterruptibly();
		try {
			for (;;) {
				if (writeCount + size <= writeArray.length) {
					insert(lines, size);
					if (writeCount >= spillSize) {
						awake.signalAll();
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
			writeLock.unlock();
		}
	}

	/**
	 * Close the synchronized lock and one inner state.
	 * 
	 */
	public void close() {
		writeLock.lock();
		try {
			closed = true;
			awake.signalAll();
		} finally {
			writeLock.unlock();
		}
	}

	
	/**
	 * 
	 * 
	 * @param	timeout
	 * 			appointed limit time
	 * 
	 * @param	unit
	 * 			time unit
	 */
	public ILine pull(long timeout, TimeUnit unit) throws InterruptedException {
		long nanoTime = unit.toNanos(timeout);
		readLock.lockInterruptibly();

		try {
			for (;;) {
				if (readCount > 0) {
					return extract();
				}

				if (nanoTime <= 0) {
					return null;
				}
				nanoTime = queueSwitch(nanoTime, true);
			}
		} finally {
			readLock.unlock();
		}
	}

	/**
	 * 
	 * @param ea    line buffer
	 *
	 * 
	 * @param	timeout
	 * 			a appointed limit time, must bigger than 0
	 * 
	 * @param	unit
	 * 			a time unit
	 * 
	 * @return
	 * 			line number of data.if less or equal than 0, means fail.
	 * 
	 * @throws	InterruptedException
	 * 			if being interrupted during the try limit time.
	 */
	public int pull(ILine[] ea, long timeout, TimeUnit unit)
			throws InterruptedException {
		long nanoTime = unit.toNanos(timeout);
		readLock.lockInterruptibly();

		try {
			for (;;) {
				if (readCount > 0) {
					return extract(ea);
				}

				if (nanoTime == -2) {
					return -1;
				}

				if (nanoTime <= 0) {
					return 0;
				}
				nanoTime = queueSwitch(nanoTime, false);
			}
		} finally {
			readLock.unlock();
		}
	}


	/**
	 * Get size of {@link IStorage} in bytes.
	 * 
	 * @return
	 * 			Storage size.
	 * 
	 * */
	@Override
	public int size() {
		return (writeCount + readCount);
	}

//	public static void main(String []args){
//		int i = 0;
//		DoubleQueue dq = new DoubleQueue(3000,1);
//		int size = 64;
//
//		for(int k = 0; k< 10; k++){
//			PullThread thread = new PullThread(dq,size);
//			thread.start();
//		}
//		ILine[] lines = new ILine[size];
//		Date date = new Date();
//		while(i<1280000/size){
//			for(int j = 0; j< size; j++){
//				ILine line = new DefaultLine();
//				line.addField("1");
//				//line.addField("default:1000\nrange:description:the block size in which the mysql data is readmandatory:falsename:blockSize");
//				lines[j] = line;
//			}
//			try {
//				dq.push(lines, size, 3000, TimeUnit.MILLISECONDS);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//			i++;
//			System.out.println(i);
//		}
//		System.out.println(new Date().getTime()-date.getTime());
//
//	}
//	static class PullThread extends Thread{
//		DoubleQueue dq;
//		int size;
//		public PullThread(DoubleQueue dq,int size){
//			this.dq = dq;
//			this.size = size;
//		}
//		@Override
//		public void run(){
//			ILine[] lines = new ILine[size];
//			try {
//				while(true) {
//					if(dq.pull(lines,100,TimeUnit.MILLISECONDS) <= 0) {
//						break;
//					}
//				}
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//	}
}

