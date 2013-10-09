package com.dp.nebula.wormhole.engine.core;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.config.JobConf;
import com.dp.nebula.wormhole.common.config.JobPluginConf;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.utils.Environment;
import com.dp.nebula.wormhole.common.utils.JobConfGenDriver;
import com.dp.nebula.wormhole.common.utils.JobDBUtil;
import com.dp.nebula.wormhole.common.utils.ParseXMLUtil;
import com.dp.nebula.wormhole.engine.config.EngineConfParamKey;
import com.dp.nebula.wormhole.engine.monitor.FailedInfo;
import com.dp.nebula.wormhole.engine.monitor.MonitorManager;
import com.dp.nebula.wormhole.engine.monitor.WormHoleJobInfo;
import com.dp.nebula.wormhole.engine.storage.StorageConf;
import com.dp.nebula.wormhole.engine.storage.StorageManager;

public class Engine {

	private static final Log s_logger = LogFactory.getLog(Engine.class);
	private static final int STATUS_CHECK_INTERVAL = 1000;
	private static final int INFO_SHOW_PERIOD = 10;
	private static final String DEFAULT_STORAGE_CLASS_NAME = "com.dp.nebula.wormhole.engine.storage.RAMStorage";
	private static final int DEFAULT_STORAGE_LINE_LIMIT = 3000;
	private static final int DEFAULT_STORAGE_BYTE_LIMIT = 1000000;
	private static final int DEFAULT_STORAGE_DESTRUCT_LIMIT = 1;
	private static final int DEFAULT_STORAGE_WAIT_TIME = 20000;
	private static final String IP = "ip";
	private static final String DB = "dbname";
	private static final String TABLE = "tableName";
	private boolean writerConsistency = false;

	private IParam engineConf;
	private Map<String, IParam> pluginReg;

	public Engine(IParam engineConf, Map<String, IParam> pluginReg) {
		this.engineConf = engineConf;
		this.pluginReg = pluginReg;
	}

	public int run(JobConf jobConf) {
		Date now = new Date();
		String source = null;
		String target = null;
		MonitorManager monitorManager = null;
		long time = 0;
		JobStatus status = JobStatus.RUNNING;
		int statusCode = status.getStatus();
		WriterManager writerManager = null;
		try {
			s_logger.info("Nebula wormhole Start");
			// create instance of StoraheManager & MonitorManager
			List<StorageConf> storageConfList = createStorageConfs(jobConf);
			if (storageConfList == null || storageConfList.isEmpty()) {
				s_logger.error("No writer is defined in job configuration or there are some errors in writer configuration");
				return JobStatus.FAILED.getStatus();
			}
			int writerNum = jobConf.getWriterNum();
			StorageManager storageManager = new StorageManager(storageConfList);
			monitorManager = new MonitorManager(writerNum);
			monitorManager.setStorageManager(storageManager);

			// get job conf and start reader & writer
			JobPluginConf readerConf = jobConf.getReaderConf();
			List<JobPluginConf> writerConf = jobConf.getWriterConfs();
			AbstractPluginManager
					.regDataSourceProp(readerConf.getPluginParam());
			// get source and target info before prepare phase, due to it may
			// throw exception during prepare
			source = readerConf.getPluginName() + "/"
					+ readerConf.getPluginParam().getValue(IP, "IP_UNKNOWN")
					+ "/" + readerConf.getPluginParam().getValue(DB, "") + "/"
					+ readerConf.getPluginParam().getValue(TABLE, "");
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < writerConf.size(); i++) {
				JobPluginConf conf = writerConf.get(i);
				AbstractPluginManager.regDataSourceProp(conf.getPluginParam());
				sb.append(conf.getPluginName())
						.append("/")
						.append(conf.getPluginParam().getValue(IP, "IPUnknown"))
						.append("/")
						.append(conf.getPluginParam().getValue(DB, ""))
						.append("/")
						.append(conf.getPluginParam().getValue(TABLE, ""));
				if (i != writerConf.size() - 1) {
					sb.append(",");
				}
			}
			target = sb.toString();

			IParam readerPluginParam = pluginReg
					.get(readerConf.getPluginName());
			s_logger.info("Start Reader Threads");
			ReaderManager readerManager = ReaderManager.getInstance(
					storageManager, monitorManager);
			readerManager.run(readerConf, readerPluginParam);

			s_logger.info("Start Writer Threads");
			writerManager = WriterManager.getInstance(storageManager,
					monitorManager, writerNum);
			writerManager.run(writerConf, pluginReg);

			int intervalCount = 0;
			int statusCheckInterval = engineConf.getIntValue(
					EngineConfParamKey.STATUS_CHECK_INTERVAL,
					STATUS_CHECK_INTERVAL);
			int monitorInfoDisplayPeriod = engineConf.getIntValue(
					EngineConfParamKey.MONITOR_INFO_DISPLAY_PERIOD,
					INFO_SHOW_PERIOD);
			writerConsistency = engineConf.getBooleanValue(
					EngineConfParamKey.WRITER_CONSISTENCY, false);

			while (true) {
				intervalCount++;
				statusCode = checkStatus(readerManager, writerManager,
						monitorManager, storageManager);
				status = JobStatus.fromStatus(statusCode);
				if (status == null) {
					s_logger.error("status = " + statusCode
							+ ".This should never happen");
					return JobStatus.FAILED.getStatus();
				}
				// read failed rollback all
				if (status == JobStatus.FAILED
						|| (status.getStatus() >= JobStatus.FAILED.getStatus() && status
								.getStatus() < JobStatus.WRITE_FAILED
								.getStatus())) {
					s_logger.error("Nebula wormhole Job is Failed!");
					writerManager.rollbackAll();
					break;
				} else if (status == JobStatus.PARTIAL_FAILED
						|| status.getStatus() >= JobStatus.WRITE_FAILED
								.getStatus()) {
					Set<String> failedIDs = getFailedWriterIDs(writerManager,
							monitorManager);
					s_logger.error("Some of the writer is Failed:"
							+ failedIDs.toString());
					writerManager.rollback(failedIDs);
					break;
				} else if (status == JobStatus.SUCCESS_WITH_ERROR) {
					s_logger.error("Nebula wormhole Job is Completed successfully with a few abnormal data");
					break;
				} else if (status == JobStatus.SUCCESS) {
					s_logger.info("Nebula wormhole Job is Completed successfully!");
					break;
				}
				// Running
				else if (status == JobStatus.RUNNING) {
					if (intervalCount % monitorInfoDisplayPeriod == 0) {
						s_logger.info(monitorManager.realtimeReport());
					}
					try {
						Thread.sleep(statusCheckInterval);
					} catch (InterruptedException e) {
						s_logger.error("Sleep of main thread is interrupted!",
								e);
					}
				}
			}
		} catch (WormholeException e) {
			if (!status.isFailed()) {
				statusCode = e.getStatusCode();
				status = JobStatus.fromStatus(e.getStatusCode());
				if (status == null) {
					s_logger.error("status = " + statusCode
							+ ".This should never happen");
					return JobStatus.FAILED.getStatus();
				}
			}

			if (JobStatus.fromStatus(e.getStatusCode()).equals(
					JobStatus.ROLL_BACK_FAILED)) {
				s_logger.error("Roll back failed: " + e.getPluginID(), e);
			} else {
				s_logger.error("Nebula wormhole Job is Failed!", e);
				try {
					writerManager.killAll();
					writerManager.rollbackAll();
				} catch (Exception e1) {
					s_logger.error("Roll back all failed ", e1);
				}
			}
		} catch (InterruptedException e) {
			status = JobStatus.FAILED;
			s_logger.error(
					"Nebula wormhole Job is Failed  as it is interrupted when prepare to read or write",
					e);
		} catch (ExecutionException e) {
			status = JobStatus.FAILED;
			s_logger.error(
					"Nebula wormhole Job is Failed  as it is failed when prepare to read or write",
					e);
		} catch (TimeoutException e) {
			status = JobStatus.FAILED;
			s_logger.error(
					"Nebula wormhole Job is Failed  as it is timeout when prepare to read or write",
					e);
		} catch (Exception e) {
			if (!status.isFailed()) {
				status = JobStatus.FAILED;
			}
			s_logger.error("Nebula wormhole Job is Failed!", e);
			s_logger.error("Unknown Exception occurs, will roll back all");
			try {
				if (writerManager != null) {
					writerManager.killAll();
					writerManager.rollbackAll();
				}
			} catch (Exception e1) {
				s_logger.error("Roll back all failed ", e1);
			}

		} finally {
			time = new Date().getTime() - now.getTime();
			if (monitorManager != null) {
				WormHoleJobInfo jobInfo = monitorManager.getJobInfo(source,
						target, time / 1000, status.getStatus(), now);
				JobDBUtil.insertOneJobInfo(jobInfo);
			}
			s_logger.info(monitorManager.finalReport());
		}
		if (statusCode != JobStatus.RUNNING.getStatus()) {
			return statusCode;
		} else {
			return status.getStatus();
		}
	}

	/**
	 * 
	 * @param readerManager
	 * @param writerManager
	 * @param monitorManager
	 * @param storageManager
	 * @return
	 */
	private int checkStatus(ReaderManager readerManager,
			WriterManager writerManager, MonitorManager monitorManager,
			StorageManager storageManager) {
		boolean readerTerminated = readerManager.terminate();
		if (readerTerminated) {
			storageManager.closeInput();
		}
		boolean writerTerminated = writerManager.terminate(writerConsistency);
		boolean readerSuccess = true;
		if (readerTerminated) {
			readerSuccess = readerManager.isSuccess();
		}
		Set<String> failedWriterIDs = new HashSet<String>();
		if (writerTerminated) {
			failedWriterIDs = writerManager.getFailedWriterID();
		}

		if (readerTerminated && writerTerminated && readerSuccess
				&& failedWriterIDs.size() == 0 && monitorManager.isJobSuccess()) {
			return JobStatus.SUCCESS.getStatus();
		} else if (readerTerminated && writerTerminated && readerSuccess
				&& failedWriterIDs.size() == 0
				&& !monitorManager.isJobSuccess()) {
			Set<FailedInfo> failedInfoSet = monitorManager.getFailedInfo();
			for (FailedInfo fi : failedInfoSet) {
				String writerID = fi.getFailedWriterID();
				int size = fi.getFailedLines();
				if (size >= 0) {
					s_logger.info(writerID + ": " + fi.getFailedLines()
							+ " lines failed to write");
				} else {
					s_logger.info(writerID + ": " + (-fi.getFailedLines())
							+ " lines duplicated to write");
				}
				if (Math.abs(fi.getFailedLines()) >= writerManager
						.getFailedLinesThreshold(writerID)) {
					if (writerConsistency) {
						return JobStatus.FAILED.getStatus();
					} else {
						return JobStatus.PARTIAL_FAILED.getStatus();
					}
				}
			}
			return JobStatus.SUCCESS_WITH_ERROR.getStatus();
		} else if ((!readerTerminated && writerTerminated)) {
			int writerErrorCode = writerManager.getErrorCode();
			if (writerErrorCode != Integer.MAX_VALUE) {
				return writerErrorCode;
			}
			return JobStatus.FAILED.getStatus();
		} else if (readerTerminated && !readerSuccess) {
			return readerManager.getStatus();
		} else if (readerTerminated && readerSuccess && writerTerminated
				&& failedWriterIDs.size() > 0) {
			int error = writerManager.getErrorCode();
			s_logger.info(error);
			if (error != Integer.MAX_VALUE) {
				return error;
			}
			if (writerConsistency) {
				return JobStatus.FAILED.getStatus();
			} else {
				return JobStatus.PARTIAL_FAILED.getStatus();
			}
		}

		return JobStatus.RUNNING.getStatus();
	}

	private Set<String> getFailedWriterIDs(WriterManager writerManager,
			MonitorManager monitorManager) {
		Set<String> r = new HashSet<String>();
		Set<FailedInfo> failedInfoSet = monitorManager.getFailedInfo();
		for (FailedInfo fi : failedInfoSet) {
			String writerID = fi.getFailedWriterID();
			if (fi.getFailedLines() >= writerManager
					.getFailedLinesThreshold(writerID)) {
				r.add(writerID);
			}
		}
		r.addAll(writerManager.getFailedWriterID());
		return r;
	}

	private List<StorageConf> createStorageConfs(JobConf jobConf) {
		List<JobPluginConf> writerConfList = jobConf.getWriterConfs();
		List<StorageConf> result = new ArrayList<StorageConf>();

		for (JobPluginConf jobPluginConf : writerConfList) {
			StorageConf storageConf = new StorageConf();
			storageConf.setId(jobPluginConf.getId());
			storageConf.setStorageClassName(engineConf.getValue(
					EngineConfParamKey.STORAGE_CLASS_NAME,
					DEFAULT_STORAGE_CLASS_NAME));
			storageConf.setLineLimit(engineConf.getIntValue(
					EngineConfParamKey.STORAGE_LINE_LIMIT,
					DEFAULT_STORAGE_LINE_LIMIT));
			storageConf.setByteLimit(engineConf.getIntValue(
					EngineConfParamKey.STORAGE_BYTE_LIMIT,
					DEFAULT_STORAGE_BYTE_LIMIT));
			storageConf.setDestructLimit(engineConf.getIntValue(
					EngineConfParamKey.STORAGE_DESTRUCT_LIMIT,
					DEFAULT_STORAGE_DESTRUCT_LIMIT));
			storageConf.setPeriod(engineConf.getIntValue(
					EngineConfParamKey.MONITOR_INFO_DISPLAY_PERIOD,
					INFO_SHOW_PERIOD));
			storageConf.setWaitTime(engineConf.getIntValue(
					EngineConfParamKey.STORAGE_WAIT_TIME,
					DEFAULT_STORAGE_WAIT_TIME));
			storageConf
					.setPeripheralTimeout(engineConf
							.getIntValue(EngineConfParamKey.READER_AND_WRITER_PERIPHERAL_TIMEOUT));
			result.add(storageConf);
		}
		return result;
	}

	public static void main(String[] args) {
		PropertyConfigurator.configure(Environment.LOG4J_CONF);

		String jobDescriptionXML = null;
		// if no parameters are passed in,
		// it generates job configure XML first
		if (args.length < 1) {
			try {
				JobConfGenDriver.generateJobConfXml();
			} catch (Exception e) {
				s_logger.error("Error in generating job configure XML: ", e);
				System.exit(JobStatus.FAILED.getStatus());
			}
			System.exit(JobStatus.SUCCESS.getStatus());
		} else if (args.length == 1) {
			jobDescriptionXML = args[0];
		}
		// return usage information
		else {
			s_logger.error("Usage: ./wormhole.sh job.xml .");
			System.exit(JobStatus.FAILED.getStatus());
		}
		JobConf jobConf = null;
		IParam engineConf = null;
		Map<String, IParam> pluginConfs = null;
		try {
			// read configurations from XML for engine & plugins
			jobConf = ParseXMLUtil.loadJobConf(jobDescriptionXML);
			engineConf = ParseXMLUtil.loadEngineConfig();
			pluginConfs = ParseXMLUtil.loadPluginConf();
			// start data transmission

		} catch (Exception e) {
			s_logger.error("Configure file error occurs: ", e);
			System.exit(JobStatus.CONF_FAILED.getStatus());
		}

		Engine engine = new Engine(engineConf, pluginConfs);
		int jobStatus = engine.run(jobConf);
		s_logger.info("wormhole return code-" + jobStatus);
		System.exit(jobStatus);
	}
}
