package com.dp.nebula.wormhole.common;

import java.util.Arrays;

import com.dp.nebula.wormhole.common.interfaces.ILine;

public class DefaultLine implements ILine{

	private String[] fieldList;
	private int length;
	private int fieldNum;
	public static final int LINE_MAX_FIELD = 1024;

	/**
	 * Construct a line with at most LINE_MAX_FIELD fields.
	 * 
	 */
	public DefaultLine() {
		this.fieldList = new String[LINE_MAX_FIELD];
	}
	
	
	/**
	 * Clear 
	 */
	public void clear() {
		length = 0;
		fieldNum = 0;
	}

	/**
	 * Get length of all fields, exclude separate.
	 * 
	 * @return
	 *			length of all fields.
	 *
	 * */
	@Override
	public int length() {
		return length;
	}

	/**
	 * Add a field into the {@link ILine}.
	 * 
	 * @param	field	
	 * 			Field added into {@link ILine}.
	 * 
	 * @return 
	 * 			true for OK, false for failure.
	 * 
	 * */
	@Override
	public boolean addField(String field) {
		fieldList[fieldNum] = field;
		fieldNum++;
		if (field != null) {
			length += field.length();
		}
		return true;
	}

	/**
	 * 	
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
	@Override
	public boolean addField(String field, int index) {
		fieldList[index] = field;
		if (fieldNum < index + 1) {
			fieldNum = index + 1;
		}
		if (field != null) {
			length += field.length();
		}
		return true;
	}

	/**
	 * Get number of total fields in the {@link ILine}.
	 * 
	 * @return
	 *			number of total fields in {@link ILine}.
	 *
	 * */
	@Override
	public int getFieldNum() {
		return fieldNum;
	}

	/**
	 * Get one field of the {@link ILine} indexed by the param.
	 * 
	 * @param	idx
	 * 			given position of the {@link ILine}.
	 * 
	 * @return
	 *			field indexed by the param.
	 *
	 * */
	@Override
	public String getField(int idx) {
		return fieldList[idx];
	}
	
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
	public String checkAndGetField(int idx) {
		if (idx < 0 || idx >= fieldNum) {
			return null;
		}
		return fieldList[idx];
	}

	/**
	 * Use param as separator of field, format the {@link ILine} into {@link StringBuffer}.
	 * 
	 * @param	separator	
	 * 			field separate.
	 * 
	 * @return
	 * 			{@link ILine} in {@link StringBuffer} style.
	 * 
	 * */
	@Override
	public StringBuffer toStringBuffer(char separator) {
		StringBuffer tmp = new StringBuffer();
		tmp.append(fieldNum);
		tmp.append(":");
		for (int i = 0; i < fieldNum; i++) {
			tmp.append(fieldList[i]);
			if (i < fieldNum - 1) {
				tmp.append(separator);
			}
		}
		return tmp;
	}
	
	/**
	 * Use param as separator of field, translate the {@link ILine} into {@link String}.
	 * 
	 * @param 	separator
	 * 			field separate.
	 * 
	 * @return
	 * 			{@link ILine} in {@link String}.
	 * 
	 * */
	@Override
	public String toString(char separator) {
		return this.toStringBuffer(separator).toString();
	}

	/**
	 *  [empty implement]<br>
	 *  Use param(separator) as separator of field, split param(linestr) and construct {@link ILine}.
	 *  
	 *  @param	lineStr
	 *			String will be translated into {@link ILine}.
	 *  
	 *  @param 	separator
	 *			field separate.
	 *  
	 *  @return	
	 *  		{@link ILine}
	 *  
	 * */
	@Override
	public ILine fromString(String lineStr, char separator) {
		return null;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(fieldList);
		result = prime * result + fieldNum;
		result = prime * result + length;
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DefaultLine other = (DefaultLine) obj;
		if (!Arrays.equals(fieldList, other.fieldList))
			return false;
		if (fieldNum != other.fieldNum)
			return false;
		if (length != other.length)
			return false;
		return true;
	}
}
