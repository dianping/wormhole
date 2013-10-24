package com.dp.nebula.wormhole.plugins.writer.mysqlwriter;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import com.dp.nebula.wormhole.plugins.common.DBUtils;

/**
 * @author renyuan.sun
 * 
 */
public class MysqlWriter extends AbstractPlugin implements IWriter {
	static final int PLUGIN_NO = 1;

	private static List<String> encodingConfigs = null;

	static {
		encodingConfigs = new ArrayList<String>();
		encodingConfigs.add("character_set_client");
		encodingConfigs.add("character_set_connection");
		encodingConfigs.add("character_set_database");
		encodingConfigs.add("character_set_results");
		encodingConfigs.add("character_set_server");
	}

	private static Map<String, String> encodingMaps = null;
	static {
		encodingMaps = new HashMap<String, String>();
		encodingMaps.put("utf-8", "UTF8");
	}

	private static final String INSERT_SQL_PATTERN = "insert into %s %s values ";

	private static final String REPLACE_SQL_PATTERN = "replace into %s %s values ";

	private static final int MAX_ERROR_COUNT = 65535;

	private static final int BLOCK_SIZE = 1000;

	private Connection conn;

	private String ip = "";

	private String sql = "";

	private String dbname = null;

	private String table = null;

	private String columns = null;

	private String encoding = null;

	private String operation = "";

	private boolean loadFile;

	private MysqlLoader loader = new MysqlLoader();

	private String writerID;

	private String[] addFields = null;

	private Logger logger = Logger.getLogger(MysqlWriter.class);

	private long failedLinesThreshold;

	private int errorCodeAdd;

	private String updateOpAppendStr = "";

	@Override
	public void init() {
		this.loadFile = getParam().getBooleanValue(ParamKey.LOADFILE, false);
		if (loadFile) {
			loader.setParam(this.getParam());
			loader.setMonitor(this.getMonitor());
			loader.init();
			return;
		}
		this.ip = getParam().getValue(ParamKey.IP, "");
		this.dbname = getParam().getValue(ParamKey.DBNAME, "");
		this.table = getParam().getValue(ParamKey.TABLE, "");
		this.columns = getParam().getValue(ParamKey.COLUMNS, "");
		this.encoding = getParam().getValue(ParamKey.ENCODING, "UTF8")
				.toLowerCase();
		this.operation = getParam().getValue(ParamKey.OPERATION, "").trim();
		if (!"insert".equalsIgnoreCase(operation)
				&& !"replace".equalsIgnoreCase(operation)
				&& !"update".equalsIgnoreCase(operation)) {
			throw new WormholeException("operation " + operation
					+ " not supported when using mysqlwriter",
					JobStatus.WRITE_FAILED.getStatus());
		}

		this.writerID = getParam().getValue(AbstractPlugin.PLUGINID, "");
		this.failedLinesThreshold = getParam().getLongValue(
				ParamKey.FAIL_LINES, 0);
		analyzeColumns();
		if (encodingMaps.containsKey(this.encoding)) {
			this.encoding = encodingMaps.get(this.encoding);
		}
		int priority = getParam().getIntValue(ParamKey.PRIORITY, 0);
		errorCodeAdd = PLUGIN_NO * JobStatus.PLUGIN_BASE + priority
				* JobStatus.WRITER_BASE;
	}

	@Override
	public void connection() {
		if (loadFile) {
			loader.connection();
			return;
		}
		try {
			conn = DBSource
					.getConnection(this.getClass(), ip, writerID, dbname);
		} catch (Exception e) {
			throw new WormholeException(e,
					JobStatus.WRITE_CONNECTION_FAILED.getStatus()
							+ errorCodeAdd);
		}
	}

	@Override
	public void write(ILineReceiver receiver) {
		if (loadFile) {
			loader.write(receiver);
			return;
		}
		try {

			/* set max count */
			this.logger.debug(String.format(
					"Config max_error_count: set max_error_count=%d",
					MAX_ERROR_COUNT));
			DBUtils.update(conn,
					String.format("set max_error_count=%d;", MAX_ERROR_COUNT));
			DBUtils.update(conn, "set session sql_mode=''");

			/* set connect encoding */
			this.logger.debug(String.format("Config encoding %s .",
					this.encoding));
			for (String encodingSql : this.makeLoadEncoding(encoding)) {
				DBUtils.update(conn, encodingSql);
			}

			/* load data begin */
			makeLoadSql();
			ILine line = null;
			StringBuilder builder = new StringBuilder(sql);
			int count = 0;
			while ((line = receiver.receive()) != null) {
				if (count != 0) {
					builder.append(",");
				}
				builder.append("(");
				builder = buildLine(line, builder);
				builder.append(")");
				count++;
				if (count == BLOCK_SIZE) {
					if ("update".equalsIgnoreCase(operation)) {
						builder.append(updateOpAppendStr);
					}
					updateOneBlock(builder.toString(), count);
					builder = new StringBuilder(sql);
					count = 0;
				}
				if (getMonitor().getFailedLines() > failedLinesThreshold) {
					logger.error(writerID
							+ " Mysqlwriter :Failed lines threshold is larger than expect");
					throw new WormholeException(
							"Mysqlwriter :Failed lines threshold is larger than expect",
							JobStatus.WRITE_FAILED.getStatus(), writerID);
				}

			}
			if (count != 0) {
				if ("update".equalsIgnoreCase(operation)) {
					builder.append(updateOpAppendStr);
				}
				updateOneBlock(builder.toString(), count);
			}
			logger.info(writerID + ": Write to mysql ends .");

		} catch (WormholeException e) {
			e.setStatusCode(e.getStatusCode() + errorCodeAdd);
			throw e;
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.WRITE_FAILED.getStatus()
					+ errorCodeAdd, writerID);
		} finally {
			if (null != this.conn) {
				try {
					conn.close();
				} catch (SQLException e1) {
					logger.error(writerID + " close failed");
					throw new WormholeException(e1,
							JobStatus.WRITE_FAILED.getStatus() + errorCodeAdd,
							writerID);
				}
			}
		}
	}

	private StringBuilder buildLine(ILine line, StringBuilder builder) {
		String field;
		int num = line.getFieldNum();

		int len = num;

		if (addFields != null) {
			len = addFields.length;
		}
		for (int i = 0, j = 0; i < len; i++) {
			if (addFields != null && addFields[i] != null) {
				field = addFields[i];
			} else if (j < num) {
				field = line.getField(j);
				j++;
			} else {
				logger.error(writerID + " write failed");
				logger.error("num:" + num + " len:" + len);
				logger.error(line.toString(','));
				throw new WormholeException(
						"MysqlWriter: Fields number is less than column number ",
						JobStatus.WRITE_FAILED.getStatus(), writerID);
			}
			if (i != 0) {
				builder.append(",");
			}
			if (field == null) {
				builder.append("null");
			} else {
				builder.append(visualSql(field));
			}
		}
		return builder;
	}

	private void updateOneBlock(String sqlStr, int count) {
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("update one block sql:" + sqlStr);
			}
			DBUtils.update(conn, sqlStr);
			getMonitor().increaseSuccessLine(count);
		} catch (SQLException e) {
			logger.warn(writerID + ": One block insert failed", e);
			logger.warn("failed sql: " + sqlStr);
			getMonitor().increaseFailedLines(BLOCK_SIZE);
		}
	}

	private void analyzeColumns() {
		if (columns.isEmpty()) {
			return;
		}
		String tmpColumns = columns;
		String[] tmpColumnsArr = tmpColumns.split(",");
		addFields = new String[tmpColumnsArr.length];
		columns = "";
		int i = 0;
		for (String field : tmpColumnsArr) {
			int index = field.indexOf("=");
			if (index != -1) {
				if (index + 1 != field.length()) {
					String columnValue = field.substring(index + 1);
					addFields[i] = columnValue;
				} else {
					addFields[i] = "";
				}
				field = field.substring(0, index).trim();
			}
			if (columns.isEmpty()) {
				columns = field;
			} else {
				columns = columns + "," + field;
			}
			i++;
		}
	}

	private void makeLoadSql() {
		if ("insert".equalsIgnoreCase(operation)) {
			sql = String.format(INSERT_SQL_PATTERN, table,
					splitColumns(columns));
		} else if ("replace".equalsIgnoreCase(operation)) {
			sql = String.format(REPLACE_SQL_PATTERN, table,
					splitColumns(columns));
		} else if ("update".equalsIgnoreCase(operation)) {
			sql = String.format(INSERT_SQL_PATTERN, table,
					splitColumns(columns));
			StringBuilder sb = new StringBuilder(" ON DUPLICATE KEY UPDATE ");
			for (String col : StringUtils.split(columns, ',')) {
				col = quoteData(col.trim());
				sb.append(col).append("=VALUES(").append(col).append("),");
			}
			updateOpAppendStr = sb.substring(0, sb.lastIndexOf(","));
		}
	}

	private String quoteData(String data) {
		if (data == null || data.trim().startsWith("`")) {
			return data;
		}
		return ('`' + data + '`');
	}

	private String visualSql(String sql) {
		String result = sql;
		StringBuilder sb = new StringBuilder();
		char[] characters = result.toCharArray();
		for (int k = 0; k < characters.length; k++) {
			if (characters[k] == '\\') {
				sb.append("\\\\");
			} else if (characters[k] == '\"') {
				sb.append("\\\"");
			} else {
				sb.append(characters[k]);
			}
		}
		result = "\"" + sb.toString() + "\"";
		return result;
	}

	/**
	 * @param der
	 *            column list from job configure
	 * @return comma split column string, can be empty string
	 */
	private String splitColumns(String colorder) {
		if (colorder.isEmpty()) {
			return "";
		}
		String[] columnArray = colorder.split(",");
		StringBuilder sb = new StringBuilder();
		for (String column : columnArray) {
			sb.append(quoteData(column.trim()) + ",");
		}
		return "(" + sb.substring(0, sb.lastIndexOf(",")) + ")";
	}

	private List<String> makeLoadEncoding(String encoding) {
		List<String> ret = new ArrayList<String>();

		String configSql = "SET %s=%s; ";
		for (String config : encodingConfigs) {
			this.logger.debug(String.format(configSql, config, encoding));
			ret.add(String.format(configSql, config, encoding));
		}

		return ret;
	}

	@Override
	public void finish() {
		if (loadFile) {
			loader.finish();
			return;
		}
	}

	public String getEncoding() {
		return encoding;
	}

	@Override
	public void commit() {
		if (loadFile) {
			loader.commit();
			return;
		}
	}

}
