package com.dp.nebula.wormhole.common.utils;

import static org.easymock.EasyMock.expect;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ StringUtil.class, System.class })
public class StringUtilTest {

	@Test
	public void testReplaceEnvironmentVariables() {
		String expectedText = "abcdedfg ${abc} abcdefg";
		String actualText = "abcdedfg cde abcdefg";

		mockStatic(System.class);
		expect(System.getenv("abc")).andReturn("cde").anyTimes();
		PowerMock.replay(System.class);
		Assert.assertEquals(actualText,
				StringUtil.replaceEnvironmentVariables(expectedText));
		PowerMock.verify(System.class);
	}
	
}
