package com.dp.nebula.wormhole.common;

import java.util.List;

import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.engine.storage.IStorage;

public class BufferedLineExchanger implements ILineSender, ILineReceiver{

	static private final int DEFAUTL_BUF_SIZE = 64;

	/**	store data which reader put to StroeageForWrite area. */
	private ILine[] writeBuf;

	/**	store data which {@link IWriter} get from StroeageForRead area. */
	private ILine[] readBuf;

	private int writeBufIdx = 0;

	private int readBufIdx = 0;

	private List<IStorage> storageForWrite;

	private IStorage storageForRead;
	
	/**
	 * Construct a {@link BufferedLineExchanger}.
	 * 
	 * @param	storageForRead
	 * 			Storage which {@link IWriter} get data from.
	 * 
	 * @param	storageForWrite
	 * 			Storage which {@link IReader} put data to.
	 * 
	 */
	public BufferedLineExchanger(IStorage storageForRead, List<IStorage> storageForWrite) {
		this(storageForRead, storageForWrite, DEFAUTL_BUF_SIZE);
	}

	/**
	 * Construct a {@link BufferedLineExchanger}.
	 * 
	 * @param	storageForRead
	 * 			Storage which {@link IWriter} get data from.
	 * 
	 * @param 	storageForWrite
	 * 			Storage which {@link IReader} put data to.
	 * 
	 * @param	bufSize
	 * 			Storage buffer size.
	 * 
	 */
	public BufferedLineExchanger(IStorage storageForRead,
			List<IStorage> storageForWrite, int bufSize) {
		this.storageForRead = storageForRead;
		this.storageForWrite = storageForWrite;
		this.writeBuf = new ILine[bufSize];
		this.readBuf = new ILine[bufSize];
	}

	/**
	 * Get next line of data which dumped to data destination.
	 * 
	 * @return
	 * 			next {@link ILine}.
	 * 
	 */
	@Override
	public ILine receive() {
		if (readBufIdx == 0) {
			readBufIdx = storageForRead.pull(readBuf);
			if (readBufIdx == 0) {
				return null;
			}
		}
		return readBuf[--readBufIdx];
	}

	/**
	 * Construct one {@link ILine} of data in {@link Storage} which will be used to exchange data.
	 * 
	 * @return 
	 * 			a new {@link ILine}.
	 * 
	 * */
	@Override
	public ILine createNewLine() {
		return new DefaultLine();
	}

	/**
	 * Put one {@link ILine} into {@link Storage}.
	 * 
	 * @param 	line	
	 * 			{@link ILine} of data pushed into {@link Storage}.
	 * 
	 * @return
	 *			true for OK, false for failure.
	 *
	 * */
	@Override
	public Boolean send(ILine line) {
		boolean result = true;
		if (writeBufIdx >= writeBuf.length) {
			if(!writeAllStorage(writeBuf, writeBufIdx)) {
				result = false;
			}
			writeBufIdx = 0;
		}
		writeBuf[writeBufIdx++] = line;
		return result;
	}

	/**
	 * Flush data in buffer (if exists) to {@link Storage}.
	 * 
	 * */
	@Override
	public void flush() {
		if (writeBufIdx > 0) {
			writeAllStorage(writeBuf, writeBufIdx);
		}
		writeBufIdx = 0;
	}

	/**
	 * Write buffered data(in a line array) to all storages which offer data to {@link IWriter}.
	 * This method is the base of double write(data dumped to multiple destinations).
	 * 
	 * @param lines
	 * 			A line array buffered data.
	 * 
	 * @param size
	 * 			Limit of the line array.
	 * 
	 * @return
	 * 			True or False represents write data to storages success or fail.
	 * 
	 */
	private boolean writeAllStorage(ILine[] lines, int size) {
		boolean result = true;
		for (IStorage s : this.storageForWrite) {
			if(!s.push(lines, size)) {
				result = false;
			}
		}
		return result;
	}
}
