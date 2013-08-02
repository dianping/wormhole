package com.dp.nebula.wormhole.plugins.reader.salesforcereader;

import java.util.Map;

import com.sforce.soap.partner.Field;

public class DescribeRefObject {

	private String objectName;
	private Map<String, Field> fieldInfoMap;

	DescribeRefObject(String objectName, Map<String, Field> fieldInfoMap) {
		this.objectName = objectName;
		this.fieldInfoMap = fieldInfoMap;
	}

	public Map<String, Field> getFieldInfoMap() {
		return fieldInfoMap;
	}

	public String getObjectName() {
		return objectName;
	}
}
