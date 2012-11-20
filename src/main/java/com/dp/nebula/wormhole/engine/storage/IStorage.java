package com.dp.nebula.wormhole.engine.storage;

import com.dp.nebula.wormhole.common.interfaces.ILine;

/**
 * A buffer where {@link IReader} and {@link IWriter} exchange data.
 * 
 * */
public interface IStorage {
	
	/**
	 * Initialization for {@link IStorage}.
	 * 
	 * @param	id
	 *          {@link IWriter} id.
	 * 
	 * @param	lineLimit
	 *          Limit of the line number the {@link IStorage} can hold.
	 * 
	 * @param	byteLimit
	 *          Limit of the bytes the {@link IStorage} can hold.
	 * 
	 * @param   destructLimit
	 *          Limit of the times the {@link IStorage} can fail.
     *
     * @return
	 * 			true for OK, false for failed.
	 * 
	 * */
	public boolean init(String id, int lineLimit, int byteLimit, int destructLimit, int waitTime);

	/**
	 * Push one line into {@link IStorage}, used by {@link IReader}
	 * 
	 * @param 	line
	 * 			One line of record, see {@link ILine}
	 * 
	 * @return
	 * 			true for OK, false for failure.
	 * 
	 * */
	public boolean push(ILine line);

	/**
	 * Push multiple lines into {@link IStorage}, used by {@link IReader}
	 * 
	 * @param 	lines
	 * 			multiple lines of records, see {@link ILine}
	 * 
	 * @param 	size
	 * 			limit of line number to be pushed.
	 * 
	 * @return
	 * 			true for OK, false for failure.
	 * 
	 * */
	public boolean push(ILine[] lines, int size);

	/**
	 * Pull one line from {@link IStorage}, used by {@link IWriter}.
	 * 
	 * @return
	 * 			one {@link ILine} of record.
	 * 
	 * */
	public ILine pull();

	/**
	 * Pull multiple lines from {@link IStorage}, used by {@link IWriter}.
	 * 
	 * @param 	lines
	 * 			an empty array which will be filled with multiple {@link ILine} as the result.
	 * 
	 * @return
	 * 			number of lines pulled。
	 * 
	 * */
	public int pull(ILine[] lines);
	
	
	public void close();
	/**
	 * Get size of {@link IStorage} in bytes
	 * 
	 * @return
	 * 			Storage size.
	 * 
	 * */
	public int size();

	/**
	 * Check {@link IStorage} is empty.
	 * 
	 * @return
	 * 			true if empty.
	 * 
	 * */
	public boolean empty();
	
	/**
	 * Get information about {@link IStorage}.
	 * 
	 * @return
	 * 			{@link IStorage} information.
	 * 
	 * */
	public String info();
	
	
	public Statistics getStat();
}
