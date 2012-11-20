package com.dp.nebula.wormhole.transform.common;

import com.dp.nebula.wormhole.common.interfaces.ITransformer;
import com.dp.nebula.wormhole.engine.utils.JarLoader;
import com.dp.nebula.wormhole.engine.utils.ReflectionUtil;



public class TransformerFactory {
	
	public static final String JAR_PATH = "transformers/";
	public static ITransformer create(String name){
		ITransformer result = ReflectionUtil.createInstanceByDefaultConstructor(
				name, 
				ITransformer.class,
				JarLoader.getInstance(JAR_PATH));
		return result;
	}
}
