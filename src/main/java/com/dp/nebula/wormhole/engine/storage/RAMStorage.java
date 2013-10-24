package com.dp.nebula.wormhole.engine.storage;

import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;

/**
 * A concrete storage which use RAM memory space to store the swap spaces. It
 * provides a high-speed,safe way to realize data exchange.
 * 
 * @see {@link IStorage}
 * @see {@link DoubleQueue}
 * @see {@link SingleQueue}
 * @see {@link BufferedLineExchanger}
 */
public class RAMStorage extends AbstractStorage {
	private static final Logger log = Logger.getLogger(RAMStorage.class);

	private StorageQueue sq = null;

	private int waitTime = 3000;

	public boolean init(String id, int lineLimit, int byteLimit,
			int destructLimit, int waitTime) {
		if (this.getStat() == null) {
			this.setStat(new Statistics(id, this));
		}
		getStat().periodPass();
		if (lineLimit <= 0 || byteLimit <= 0) {
			log.error("Error: line or byte limit is less than or equal to 0.");
			return false;
		}
		setPushClosed(false);
		this.waitTime = waitTime;
		this.setDestructLimit(destructLimit);
		this.sq = new DoubleQueue(lineLimit, byteLimit);
		// this.mars = new SingleQueue(lineLimit,byteLimit);
		return true;
	}

	/**
	 * Push one line into {@link IStorage}, used by {@link IReader}
	 * 
	 * @param line
	 *            One line of record which will push into storage, see
	 *            {@link ILine}
	 * 
	 * @return true for OK, false for failure.
	 * 
	 * */
	@Override
	public boolean push(ILine line) {
		if (getPushClosed()) {
			return false;
		}
		try {
			while (!sq.push(line, waitTime, TimeUnit.MILLISECONDS)) {
				getStat().incLineRRefused(1);
				if (getDestructLimit() > 0
						&& getStat().getLineRRefused() >= getDestructLimit()) {
					if (getPushClosed()) {
						log.warn("Close RAMStorage for " + getStat().getId()
								+ ". Queue:" + info() + " Timeout times:"
								+ getStat().getLineRRefused());
						setPushClosed(true);
					}
					throw new WormholeException("",
							JobStatus.WRITE_OUT_OF_TIME.getStatus());
				}
			}
		} catch (InterruptedException e) {
			return false;
		}
		return true;
	}

	/**
	 * Push multiple lines into {@link IStorage}, used by {@link IReader}
	 * 
	 * @param lines
	 *            multiple lines of records which will push into storage, see
	 *            {@link ILine}
	 * 
	 * @param size
	 *            limit of line number to be pushed.
	 * 
	 * @return true for OK, false for failure.
	 * 
	 * */
	@Override
	public boolean push(ILine[] lines, int size) {
		if (getPushClosed()) {
			return false;
		}

		try {
			while (!sq.push(lines, size, waitTime, TimeUnit.MILLISECONDS)) {
				getStat().incLineRRefused(1);
				if (getDestructLimit() > 0
						&& getStat().getLineRRefused() >= getDestructLimit()) {
					if (!getPushClosed()) {
						log.warn("Close RAMStorage for " + getStat().getId()
								+ ". Queue:" + info() + " Timeout times:"
								+ getStat().getLineRRefused());
						setPushClosed(true);
					}
					throw new WormholeException("",
							JobStatus.WRITE_OUT_OF_TIME.getStatus());
				}
			}
		} catch (InterruptedException e) {
			return false;
		}

		getStat().incLineRx(size);
		for (int i = 0; i < size; i++) {
			getStat().incByteRx(lines[i].length());
		}

		return true;
	}

	/**
	 * Pull one line from {@link IStorage}, used by {@link IWriter}
	 * 
	 * @return one {@link ILine} of record.
	 * 
	 * */
	@Override
	public ILine pull() {
		ILine line = null;
		try {
			while ((line = sq.pull(waitTime, TimeUnit.MILLISECONDS)) == null) {
				getStat().incLineTRefused(1);
			}
		} catch (InterruptedException e) {
			return null;
		}
		if (line != null) {
			getStat().incLineTx(1);
			getStat().incByteTx(line.length());
		}
		return line;
	}

	/**
	 * Pull multiple lines from {@link IStorage}, used by {@link IWriter}
	 * 
	 * @param lines
	 *            an empty array which will be filled with multiple
	 *            {@link ILine} as the result.
	 * 
	 * @return number of lines pulled
	 * 
	 * */
	@Override
	public int pull(ILine[] lines) {
		int readNum = 0;
		try {
			while ((readNum = sq.pull(lines, waitTime, TimeUnit.MILLISECONDS)) == 0) {
				getStat().incLineTRefused(1);
				if (getDestructLimit() > 0
						&& getStat().getLineTRefused() >= getDestructLimit()) {
					if (!getPushClosed()) {
						log.warn("Close RAMStorage for " + getStat().getId()
								+ ". Queue:" + info() + " Timeout times:"
								+ getStat().getLineRRefused());
						setPushClosed(true);
					}
					throw new WormholeException("",
							JobStatus.READ_OUT_OF_TIME.getStatus());
				}
			}
		} catch (InterruptedException e) {
			return 0;
		}
		if (readNum > 0) {
			getStat().incLineTx(readNum);
			for (int i = 0; i < readNum; i++) {
				getStat().incByteTx(lines[i].length());
			}
		}
		if (readNum == -1) {
			return 0;
		}
		return readNum;
	}

	/**
	 * Get the used byte size of {@link IStorage}.
	 * 
	 * @return Used byte size of storage.
	 * 
	 */
	public int size() {
		return sq.size();
	}

	/**
	 * Check whether the storage space is empty or not.
	 * 
	 * @return true if empty, false if not empty.
	 * 
	 */
	public boolean empty() {
		return (size() <= 0);
	}

	/**
	 * Get line number of the {@link IStorage}
	 * 
	 * @return Limit of the line number the {@link IStorage} can hold.
	 * 
	 */
	public int getLineLimit() {
		return sq.getLineLimit();
	}

	/**
	 * Get info of line number in {@link IStorage} space.
	 * 
	 * @return Information of line number.
	 * 
	 */
	@Override
	public String info() {
		return sq.info();
	}

	/**
	 * Set push state closed.
	 * 
	 */
	@Override
	public void close() {
		setPushClosed(true);
		sq.close();
	}
}
