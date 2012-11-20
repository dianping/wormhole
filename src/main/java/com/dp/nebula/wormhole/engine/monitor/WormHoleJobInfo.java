package com.dp.nebula.wormhole.engine.monitor;

import java.util.Date;

import com.dp.nebula.common.utils.DateHelper;

public class WormHoleJobInfo {
	
	private String userName;
	private String dataSource;
	private String dataTarget;
	private Date startTime;
	private long totalLines;
	private long totalBytes;
	private long time;
	private int resultCode;
	
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getDataSource() {
		return dataSource;
	}
	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}
	public String getDataTarget() {
		return dataTarget;
	}
	public void setDataTarget(String dataTarget) {
		this.dataTarget = dataTarget;
	}
	public long getTotalLines() {
		return totalLines;
	}
	public void setTotalLines(long totalLines) {
		this.totalLines = totalLines;
	}
	public long getTotalBytes() {
		return totalBytes;
	}
	public void setTotalBytes(long totalBytes) {
		this.totalBytes = totalBytes;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	public Date getStartTime() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	public int getResultCode() {
		return resultCode;
	}
	public void setResultCode(int resultCode) {
		this.resultCode = resultCode;
	}
	public WormHoleJobInfo() {
	}

	@Override
	public String toString() {
		return "WormHoleJobInfo [dataSource=" + dataSource + ", dataTarget="
				+ dataTarget + ", resultCode=" + resultCode + ", startTime="
				+ startTime + ", time=" + time + ", totalBytes=" + totalBytes
				+ ", totalLines=" + totalLines + ", userName=" + userName + "]";
	}
	
	
	public String getString() {
		return "(\"" + dataSource + "\",\""
				+ dataTarget + "\"," + resultCode + ","
				+ time + "," + totalBytes + ","
				+ totalLines + ",\"" + userName +"\",\""
				+ DateHelper.format(startTime, DateHelper.DATE_FORMAT_PATTERN_YEAR_MONTH_DAY_HOUR_MINUTE_SECOND) + "\")";
	}
	public WormHoleJobInfo(String userName, String dataSource,
			String dataTarget, Date startTime, long totalLines,
			long totalBytes, long time, int resultCode) {
		super();
		this.userName = userName;
		this.dataSource = dataSource;
		this.dataTarget = dataTarget;
		this.startTime = startTime;
		this.totalLines = totalLines;
		this.totalBytes = totalBytes;
		this.time = time;
		this.resultCode = resultCode;
	}
}
