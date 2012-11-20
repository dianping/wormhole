package com.dp.nebula.wormhole.common;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.interfaces.IParam;

public class DefaultParam implements IParam{
	private Map<String, String> params;
	private static final Log s_logger = LogFactory.getLog(DefaultParam.class); 
	
	
	private void warn (String oldStr, Object defStr) {
		if(oldStr != null && !oldStr.isEmpty()) {
			s_logger.warn("Cannot use value:" + oldStr + ",use default value:"+ (defStr==null?null:defStr.toString()));
		}
			
	}
	public DefaultParam(Map<String, String> params) {
		this.params = params;
	}

	@Override
	public String getValue(String key) {
		return params.get(key);
	}

	@Override
	public String getValue(String key, String defaultValue) {
		String result = params.get(key);
		if(result == null || result.isEmpty()) {
			warn(result,defaultValue);
			return defaultValue;
		} else {
			return result;
		}
	}

	@Override
	public char getCharValue(String key) {
		return getCharValue(key,(char)-1);
	}

	@Override
	public char getCharValue(String key, char defaultValue) {
		String result = params.get(key);
		if(result == null || result.isEmpty()) {
			warn(result,defaultValue);
			return defaultValue;
		} else{
			try{
				return changeChar(result);
			}catch(IllegalArgumentException e){
				warn(result,defaultValue);
				return defaultValue;
			}
		}
	}

	@Override
	public int getIntValue(String key) {
		return getIntValue(key,-1);
	}

	@Override
	public int getIntValue(String key, int defaultValue) {
		String result = params.get(key);
		int resultInt = -1;
		try{
			resultInt = Integer.parseInt(result);
		}catch(Exception e){
			warn(result,defaultValue);
			return defaultValue;
		}
		return resultInt;
	}

	@Override
	public boolean getBooleanValue(String key) {
		return getBooleanValue(key,false);
	}

	@Override
	public boolean getBooleanValue(String key, boolean defaultValue) {
		String result = params.get(key);
		if(result != null && result.trim().toLowerCase().equals("true")) {
			return true;
		} else if(result != null && result.trim().toLowerCase().equals("false")){
			return false;
		} else {
			warn(result,defaultValue);
			return defaultValue;
		}
	}

	@Override
	public long getLongValue(String key) {
		return getLongValue(key,-1);
	}

	@Override
	public long getLongValue(String key, long defaultValue) {
		String result = params.get(key);
		long resultLong = -1;
		try{
			resultLong = Long.parseLong(result);
		}catch(Exception e){
			warn(result,defaultValue);
			return defaultValue;
		}
		return resultLong;
	}

	@Override
	public double getDoubleValue(String key) {
		return getDoubleValue(key, 0);
	}

	@Override
	public double getDoubleValue(String key, double defaultValue) {
		String result = params.get(key);
		double resultDouble = -1;
		try{
			resultDouble = Double.parseDouble(result);
		}catch(Exception e){
			warn(result,defaultValue);
			return defaultValue;
		}
		return resultDouble;
	}

	@Override
	public List<Character> getCharList(String key) {
		return getCharList(key,null);
	}
	
	@Override
	public List<Character> getCharList(String key, List<Character> defaultValue) {
		String values = params.get(key);
		List<Character> result = new ArrayList<Character>();
		if(values == null || values.isEmpty()) {
			warn(values,defaultValue);
			return defaultValue;
		}
		for (String item:values.split(":")) {
			try{
				result.add(changeChar(item));
			}catch(IllegalArgumentException e){
				warn(values,defaultValue);
				return defaultValue;
			}
		}
		return result;
	}
	
	@Override
	public void mergeTo(IParam param) {
	}

	@Override
	public void mergeTo(Collection<IParam> paramCollection) {
	}

	@Override
	public void putValue(String key, String value) {
		params.put(key, value);
	}
	
	public IParam clone(){
		Map<String,String> map  = new HashMap<String,String>();
		IParam result = new DefaultParam(map);
		for(String key : params.keySet()){
			map.put(key, params.get(key));
		}
		return result;
	}
	
	private char changeChar(String str) {
		char out = '\001';
		if (str != null) {
			if (str.equals("\\t")) {
				out = '\t';
			} else if (str.equals("\\n")) {
				out = '\n';
			} else {
				char[] ch = str.toCharArray();
				if (ch.length == 0) {
					throw new IllegalArgumentException();
				} else if (ch.length == 1) {
					out = ch[0];
				} else if (ch.length > 1) {
					if (str.startsWith("\\u") && str.length() == 6) {
						try {
							out = (char) Integer.valueOf(str.substring(2)).intValue();
						} catch (NumberFormatException e) {
							throw new IllegalArgumentException(e);
						}
					} else if  (str.startsWith("\\")) {
						try {
							out = (char) Integer.valueOf(str.substring(1)).intValue();
						} catch (NumberFormatException e) {
							throw new IllegalArgumentException(e);
						}
					} else {
						throw new IllegalArgumentException(String.format(
								"Cannot convert literal %s to char type", str));
					}
				}
			}
		}
		return out;
	}
}
