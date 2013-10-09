package com.dp.nebula.wormhole.engine.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReflectionUtil {

	private static final Log s_logger = LogFactory.getLog(ReflectionUtil.class);

	@SuppressWarnings("unchecked")
	public static <T> T createInstanceByDefaultConstructor(String className, Class<T> type, 
			JarLoader jarLoader){
		try{
			Class<T> clazz = null;
			if(jarLoader != null){
				clazz = (Class<T>)jarLoader.loadClass(className);
			}
			if(clazz == null){
				clazz = (Class<T>) Class.forName(className);
			}

			return clazz.newInstance();
		} 
		catch(Exception e){
			s_logger.error("Exception occurs when creating " + className , e);
			return null;
		}
	}


	public static<T> T createInstanceByDefaultConstructor(String className, Class<T> type){
		return createInstanceByDefaultConstructor(className, type, null);
	}



}
