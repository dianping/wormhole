package com.dp.nebula.wormhole.plugins.common;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;


public final class SFTPUtils {
	private static Map<String, String> fileTypeSuffixMap = null;
	
	private SFTPUtils(){
	}
	
	public enum FileType {
		TXT, COMP_TXT
	}
	
	static{
		fileTypeSuffixMap = new HashMap<String, String>();
		fileTypeSuffixMap.put("gz", "org.apache.hadoop.io.compress.GzipCodec");
		fileTypeSuffixMap.put("gzip", "org.apache.hadoop.io.compress.GzipCodec");
		fileTypeSuffixMap.put("lzo", "com.hadoop.compression.lzo.LzopCodec");
	}
	
 	public static Configuration getConf(){
 		Configuration cfg = new Configuration();
 		cfg.setClassLoader(SFTPUtils.class.getClassLoader());
		cfg.set("io.compression.codecs", 
				"org.apache.hadoop.io.compress.GzipCodec," +
				"org.apache.hadoop.io.compress.DefaultCodec," +
				"com.hadoop.compression.lzo.LzoCodec," +
				"com.hadoop.compression.lzo.LzopCodec," +
				"org.apache.hadoop.io.compress.BZip2Codec");
 		return cfg;
 	}
}
