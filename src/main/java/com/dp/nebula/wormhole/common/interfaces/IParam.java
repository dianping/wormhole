package com.dp.nebula.wormhole.common.interfaces;

import java.util.Collection;
import java.util.List;

public interface IParam extends Cloneable{
	
	String getValue(String key);
	
	String getValue(String key, String defaultValue);
	
	char getCharValue(String key);
	
	char getCharValue(String key, char defaultValue);
	
	int getIntValue(String key);
	
	int getIntValue(String key, int defaultValue);
	
	boolean getBooleanValue(String key);
	
	boolean getBooleanValue(String key, boolean defaultValue);
	
	long getLongValue(String key);
	
	long getLongValue(String key, long defaultValue);
	
	double getDoubleValue(String key);
	
	double getDoubleValue(String key, double defaultValue);
	
	List<Character> getCharList(String key);
	
	List<Character> getCharList(String key,List<Character> list); 
	
	void putValue(String key, String value);
	
	void mergeTo(IParam param);
	
	void mergeTo(Collection<IParam> paramCollection);
	
	IParam clone();
}