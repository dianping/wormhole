package com.dp.nebula.wormhole.common.interfaces;


public interface ILine {
	
	/**
	 * Add a field into the {@link ILine}.
	 * 
	 * @param	field	
	 * 			Field added into {@link ILine}.
	 * @return 
	 * 			true for OK, false for failure.
	 * 
	 * */
	boolean addField(String field);
	
	/**
	 * Add a field into the {@link ILine}.
	 * 
	 * @param	field	
	 * 			field added into {@link ILine}.
	 * 
	 * @param 	index	
	 * 			given position of field in the {@link ILine}.
	 * 
	 * @return 
	 *			true for OK, false for failure.
	 *
	 * */
	boolean addField(String field, int index);
	
	/**
	 * Get one field of the {@link ILine} indexed by the param.
	 * 
	 * NOTE:
	 * if index specifed by user beyond field number of {@link ILine}
	 * it may throw runtime excepiton
	 * 
	 * 
	 * @param	 idx
	 * 			given position of the {@link ILine}.
	 * 
	 * @return
	 *			field indexed by the param.
	 *
	 * */
	String getField(int idx);
	
	/**
	 * Get one field of the {@link ILine} indexed by the param.
	 * if idx specified by user beyond field number of {@link ILine}
	 * null will be returned
	 * 
	 * @param	 idx
	 * 			given position of the {@link ILine}.
	 * 
	 * @return
	 *			field indexed by the param.
	 *
	 * */
	String checkAndGetField(int idx);
	
	/**
	 * Get number of total fields in the {@link ILine}.
	 * 
	 * @return
	 *			number of total fields in {@link ILine}.
	 *
	 * */
	int getFieldNum();
	
	/**
	 * Use param as separator of field, format the {@link ILine} into {@link StringBuffer}.
	 * 
	 * @param	separator	
	 * 			field separator.
	 * 
	 * @return
	 * 			{@link ILine} in {@link StringBuffer} style.
	 * 
	 * */
	StringBuffer toStringBuffer(char separator);
	
	/**
	 * Use param as separator of field, translate the {@link ILine} into {@link String}.
	 * 
	 * @param 	separator
	 * 			field separator.
	 * 
	 * @return
	 * 			{@link ILine} in {@link String}.
	 * 
	 * */
	String toString(char separator);
	
	/**
	 *  Use param(separator) as separator of field, split param(linestr) and construct a {@link ILine}.
	 *  
	 *  @param 	lineStr
	 *				String will be translated into {@link ILine}.
	 *  
	 *  @param 	separator
	 *				field separate.
	 *  
	 *  @return	
	 *  			{@link ILine}
	 *  
	 * */
	ILine fromString(String lineStr, char separator);
	
	/**
	 * Get length of all fields, exclude separate.
	 * 
	 * @return
	 *			length of all fields.
	 *
	 * */
	int length();
	
}
