package com.dp.nebula.wormhole.plugins.writer.mysqlwriter;

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.*;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.DBSource;

public class MysqlLoader extends AbstractPlugin implements IWriter {
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

	private static final int MAX_ERROR_COUNT = 65535;

	private int errorCodeAdd;

	private Connection conn;

	private String ip = "";

	private String dbname = null;

	private String table = null;

	private String columns = null;

	private String encoding = null;

	private String replace = "";

	private Logger logger = Logger.getLogger(MysqlWriter.class);

	private char sep = '\t';

	private char line = '\n';

	private String writerID;

	private String[] addFields = null;

	private long failedLinesThreshold;

	@Override
	public void init() {
		this.ip = getParam().getValue(ParamKey.IP, "");
		this.dbname = getParam().getValue(ParamKey.DBNAME, "");
		this.table = getParam().getValue(ParamKey.TABLE, "");
		this.columns = getParam().getValue(ParamKey.COLUMNS, "");
		this.encoding = getParam().getValue(ParamKey.ENCODING, "UTF8")
				.toLowerCase();
		analyzeColumns();
		String operation = getParam().getValue(ParamKey.OPERATION, "").trim();
		if (!"insert".equalsIgnoreCase(operation)
				&& !"replace".equalsIgnoreCase(operation)) {
			throw new WormholeException("operation " + operation
					+ " not supported when using mysqlloader",
					JobStatus.WRITE_FAILED.getStatus());
		}

		this.replace = "replace".equalsIgnoreCase(operation) ? "replace" : "";
		if (encodingMaps.containsKey(this.encoding)) {
			this.encoding = encodingMaps.get(this.encoding);
		}
		this.writerID = getParam().getValue(AbstractPlugin.PLUGINID, "");
		this.failedLinesThreshold = getParam().getLongValue(
				ParamKey.FAIL_LINES, 0);
		int priority = getParam().getIntValue(ParamKey.PRIORITY, 0);
		errorCodeAdd = MysqlWriter.PLUGIN_NO * JobStatus.PLUGIN_BASE + priority
				* JobStatus.WRITER_BASE;
	}

	@Override
	public void connection() {
		try {
			conn = DBSource.getConnection(MysqlWriter.class, ip, writerID,
					dbname);
		} catch (Exception e) {
			throw new WormholeException(e,
					JobStatus.WRITE_CONNECTION_FAILED.getStatus()
							+ errorCodeAdd);
		}
	}

	@Override
	public void write(ILineReceiver receiver) {
		try {
			conn = DBSource.getConnection(MysqlWriter.class, ip, writerID,
					dbname);
			com.mysql.jdbc.Statement stmt = (com.mysql.jdbc.Statement) ((org.apache.commons.dbcp.DelegatingConnection) this.conn)
					.getInnermostDelegate().createStatement(
							ResultSet.TYPE_SCROLL_INSENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
			/* set max count */
			this.logger.debug(String.format(
					"Config max_error_count: set max_error_count=%d",
					MAX_ERROR_COUNT));
			stmt.executeUpdate(String.format("set max_error_count=%d;",
					MAX_ERROR_COUNT));

			/* set connect encoding */
			this.logger.debug(String.format("Config encoding %s .",
					this.encoding));
			for (String encodingSql : this.makeLoadEncoding(encoding)) {
				stmt.execute(encodingSql);
			}

			/* load data begin */
			String loadSql = this.makeLoadSql();
			this.logger.info(String.format(writerID + ": Load sql: %s.",
					loadSql));

			MysqlWriterInputStreamAdapter localInputStream = new MysqlWriterInputStreamAdapter(
					writerID, receiver, this, addFields);
			stmt.setLocalInfileInputStream(localInputStream);
			stmt.executeUpdate(loadSql);
			getMonitor().increaseSuccessLine(localInputStream.getLineNumber());
			stmt.close();
			this.logger.info(writerID + ": Wormhole write to mysql ends .");

		} catch (WormholeException e) {
			e.setStatusCode(e.getStatusCode() + errorCodeAdd);
			throw e;
		} catch (Exception e) {
			logger.error(writerID + " write failed");
			throw new WormholeException(e, JobStatus.WRITE_FAILED.getStatus()
					+ errorCodeAdd, writerID);
		} finally {
			try {
				conn.close();
			} catch (Exception e) {
				logger.error(writerID + " close connection failed");
			}
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

	private String quoteData(String data) {
		if (data == null || data.trim().startsWith("@")
				|| data.trim().startsWith("`")) {
			return data;
		}
		return ('`' + data + '`');
	}

	// colorder can not be null
	private String splitColumns(String colorder) {
		String[] columnArray = colorder.split(",");
		StringBuilder sb = new StringBuilder();
		for (String column : columnArray) {
			sb.append(quoteData(column.trim()) + ",");
		}
		return sb.substring(0, sb.lastIndexOf(","));
	}

	private String makeLoadSql() {
		String sql = "LOAD DATA LOCAL INFILE '`sunny`' " + this.replace
				+ " INTO TABLE ";
		// fetch table
		sql += this.quoteData(this.table);
		// fetch charset
		sql += " CHARACTER SET " + this.encoding;
		// fetch records
		// sql +=
		// String.format(" FIELDS TERMINATED BY '\001' ESCAPED BY '\\' ");
		sql += String.format(" FIELDS TERMINATED BY '%c'  ESCAPED BY '\\\\' ",
				this.sep);
		// fetch lines
		// sql += String.format(" LINES TERMINATED BY '\002' ");
		sql += String.format(" LINES TERMINATED BY '%c'", this.line);
		if (failedLinesThreshold > 0) {
			sql += String.format(" IGNORE %d LINES", failedLinesThreshold);
		}
		// fetch colorder
		if (this.columns != null && !this.columns.trim().isEmpty()) {
			sql += "(" + splitColumns(this.columns) + ")";
		}
		// add set statement
		sql += ";";
		return sql;
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

	public String getEncoding() {
		return encoding;
	}

	@Override
	public void commit() {
	}
}
