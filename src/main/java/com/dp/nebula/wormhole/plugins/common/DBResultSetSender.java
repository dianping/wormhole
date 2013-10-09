package com.dp.nebula.wormhole.plugins.common;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;


public class DBResultSetSender {
	
	private ILineSender sender;

	private int columnCount;
	
	private IPluginMonitor monitor;

	private Map<String, SimpleDateFormat> dateFormatMap = new HashMap<String, SimpleDateFormat>();

	private SimpleDateFormat[] timeMap = null;

	private static final Log s_logger = LogFactory.getLog(DBResultSetSender.class);

	public static DBResultSetSender newSender(ILineSender sender) {
		return new DBResultSetSender(sender);
	}

	public DBResultSetSender(ILineSender lineSender) {
		this.sender = lineSender;
	}

	public void setMonitor(IPluginMonitor iMonitor) {
		this.monitor = iMonitor;
	}
	
	public void setDateFormatMap(Map<String, SimpleDateFormat> dateFormatMap) {
		this.dateFormatMap = dateFormatMap;
	}

	public void sendToWriter(ResultSet resultSet) throws SQLException{
		String item = null;
		Timestamp ts = null;
		setColumnCount(resultSet.getMetaData().getColumnCount());
		setColumnTypes(resultSet);
		while (resultSet.next()) {
			ILine line = sender.createNewLine();
			try {
				/* TODO: date format need to handle by transfomer plugin */
				for (int i = 1; i <= columnCount; i++) {
					if (null != timeMap[i]) {
						ts = resultSet.getTimestamp(i);
						if (null != ts) {
							item = timeMap[i].format(ts);
						} else {
							item = null;
						}
					} else {
						item = resultSet.getString(i);
					}
					line.addField(item);
				}
				Boolean b = sender.send(line);
				if (null != monitor) {
					if (b) {
						monitor.increaseSuccessLines();
					} else {
						monitor.increaseFailedLines();
					}
				}
			} catch (SQLException e) {
				monitor.increaseFailedLines();
				s_logger.error(e.getMessage() + "| One dirty line : " + line.toString('\t'));
			}
		}
		
	}

	public void flush() {
		if (sender != null) {
			sender.flush();
		}
	}
	
	private void setColumnTypes(ResultSet resultSet) throws SQLException {
		timeMap = new SimpleDateFormat[columnCount + 1];

		ResultSetMetaData rsmd = resultSet.getMetaData();
		
		for (int i = 1; i <= columnCount; i++) {
			String type = rsmd.getColumnTypeName(i).toLowerCase().trim();
			if (this.dateFormatMap.containsKey(type)) {
				timeMap[i] = this.dateFormatMap.get(type);
			}
		}
	}
	
	private void setColumnCount(int columnCount) {
		this.columnCount = columnCount;
	}

}
