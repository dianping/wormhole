package com.dp.nebula.wormhole.plugins.common;

import org.postgresql.util.PSQLException;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;

public final class ErrorCodeUtils {
	
	private ErrorCodeUtils() {
		
	}
	
	public static void psqlWriterWrapper (PSQLException pe,WormholeException we) {
		if (pe.getSQLState().startsWith("57")) {
			we.setStatusCode(JobStatus.WRITE_OPERATOR_INTERVENTION.getStatus());
		} else if (pe.getSQLState().startsWith("5")||pe.getSQLState().startsWith("F0")
				||pe.getSQLState().startsWith("P0")||pe.getSQLState().startsWith("XX")) {
			we.setStatusCode(JobStatus.WRITE_SYSTEM_ERROR.getStatus());
		} else if (pe.getSQLState().startsWith("2")||pe.getSQLState().startsWith("3")||pe.getSQLState().startsWith("4")) {
			we.setStatusCode(JobStatus.WRITE_DATA_EXCEPTION.getStatus());
		} else {
			we.setStatusCode(JobStatus.WRITE_FAILED.getStatus());
		}
	}
	public static void psqlReaderWrapper (PSQLException pe,WormholeException we) {
		if (pe.getSQLState().startsWith("57")) {
			we.setStatusCode(JobStatus.READ_OPERATOR_INTERVENTION.getStatus());
		} else if (pe.getSQLState().startsWith("5")||pe.getSQLState().startsWith("F0")
				||pe.getSQLState().startsWith("P0")||pe.getSQLState().startsWith("XX")) {
			we.setStatusCode(JobStatus.READ_SYSTEM_ERROR.getStatus());
		} else if (pe.getSQLState().startsWith("2")||pe.getSQLState().startsWith("3")||pe.getSQLState().startsWith("4")) {
			we.setStatusCode(JobStatus.READ_DATA_EXCEPTION.getStatus());
		} else {
			we.setStatusCode(JobStatus.READ_FAILED.getStatus());
		}
	}
}
