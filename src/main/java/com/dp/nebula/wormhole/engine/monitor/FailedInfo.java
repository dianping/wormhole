package com.dp.nebula.wormhole.engine.monitor;

public class FailedInfo {
	
	private String failedWriterID;
	private int failedLines;

	public FailedInfo(String failedWriterID, int failedLines) {
		super();
		this.failedWriterID = failedWriterID;
		this.failedLines = failedLines;
	}
	
	public String getFailedWriterID() {
		return failedWriterID;
	}
	public int getFailedLines() {
		return failedLines;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + failedLines;
		result = prime * result
				+ ((failedWriterID == null) ? 0 : failedWriterID.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		FailedInfo other = (FailedInfo) obj;
		if (failedLines != other.failedLines) {
			return false;
		}
		if (failedWriterID == null) {
			if (other.failedWriterID != null) {
				return false;
			}
		} else if (!failedWriterID.equals(other.failedWriterID)) {
			return false;
		}
		return true;
	}
}
