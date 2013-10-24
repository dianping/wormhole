package com.dp.nebula.wormhole.plugins.reader.hivereader;

public enum HiveReaderMode {
	READ_FROM_HIVESERVER ("READ_FROM_HIVESERVER"),
	READ_FROM_HDFS ("READ_FROM_HDFS");
	
	private String mode = null;
	private HiveReaderMode(String mode) {
		this.mode = mode;
	}
	String getMode(){
		return mode;
	}
}
