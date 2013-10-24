package com.dp.nebula.wormhole.engine.common;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.Test;

public class TestUtils {
	
	public static String getResourcePath(String[] paths){
		StringBuilder sb = new StringBuilder();
		sb.append("src").append(File.separator).append("test").
		   append(File.separator).append("resources").append(File.separator);
		for(int i = 0; i < paths.length; i++){
			if(i > 0){
				sb.append(File.separator);
			}
			sb.append(paths[i]);
		}
		return sb.toString();
	}
	@Test
	public void testGetResourcePath() {
		assertEquals("src"+File.separator+ "test"+File.separator+ "resources"+File.separator+ "jar"+File.separator+ "path01",getResourcePath(new String[] {"jar", "path01"}));
	}

}
