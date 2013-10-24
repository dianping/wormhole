package com.dp.nebula.wormhole.engine.utils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;

import com.dp.nebula.wormhole.engine.common.TestUtils;

public class JarLoaderTest {
	
	@Test
	public void testGetInstance_with_one_path_success(){
		JarLoader jl = JarLoader.getInstance(getPath(new String[] {"jar", "path01"}));
		assertNotNull(jl);
	}
	
	@Test
	public void testGetInstance_with_one_path_failed(){
		JarLoader jl = JarLoader.getInstance("httq://123.123.12.cn");
		assertNull(jl);
	}
	
	@Test
	public void testGetInstance_with_multiple_paths_success(){
		JarLoader jl = JarLoader.getInstance(
				new String[] {getPath(new String[] {"jar", "path01"}),
							  getPath(new String[] {"jar", "path02"})}
				);
		assertNotNull(jl);
	}
	
	@Test
	public void testGetInstance_with_multiple_paths_failed(){
		JarLoader jl = JarLoader.getInstance(
				new String[] {getPath(new String[] {"jar", "path01"}),
							 "httq://aaaa"}
				);
		assertNull(jl);
	}

	private String getPath(String[] paths){
		return TestUtils.getResourcePath(paths);
	}

}
