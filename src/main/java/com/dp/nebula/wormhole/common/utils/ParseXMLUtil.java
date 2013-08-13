package com.dp.nebula.wormhole.common.utils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;

import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.config.JobConf;
import com.dp.nebula.wormhole.common.config.JobPluginConf;
import com.dp.nebula.wormhole.common.interfaces.IParam;

/**
 * A utility tool to parse configure XML file
 */
public final class ParseXMLUtil {
	private static final Logger LOG = Logger.getLogger(ParseXMLUtil.class);

	private ParseXMLUtil() {
	}

	/**
	 * @param filename
	 *            job configuration name
	 * @return a job configure instance with the specified job configuration
	 *         file
	 */
	@SuppressWarnings("unchecked")
	public static JobConf loadJobConf(String filename) {
		JobConf job = new JobConf();
		Document document = null;

		try {
			String xml = FileUtils
					.readFileToString(new File(filename), "UTF-8");
			xml = StringUtil.replaceEnvironmentVariables(xml);
			document = DocumentHelper.parseText(xml);
		} catch (IOException e) {
			LOG.error(String.format("WormHole can't find job conf file: %s.",
					filename));
		} catch (DocumentException e) {
			LOG.error(String.format("Parse %s to document failed.", filename));
		}

		String xpath = "/job";

		Element jobE = (Element) document.selectSingleNode(xpath);
		String jobId = jobE.attributeValue("id", "WormHole_id_not_found")
				.trim();
		job.setId(jobId);

		JobPluginConf readerJobConf = new JobPluginConf();
		Element readerE = (Element) jobE.selectSingleNode(xpath + "/reader");
		Element readerPluginE = (Element) readerE.selectSingleNode("plugin");
		String readerId = readerE.attributeValue("id");
		String readerName = readerPluginE.getStringValue().trim().toLowerCase();
		readerJobConf.setPluginName(readerName);
		readerJobConf.setId(readerId == null ? "reader-id-" + readerName
				: readerId.trim());
		Map<String, String> readerPluginParamMap = getParamMap(readerE);

		IParam readerPluginParam = new DefaultParam(readerPluginParamMap);
		readerJobConf.setPluginParam(readerPluginParam);

		List<JobPluginConf> writerJobConfs = new ArrayList<JobPluginConf>();
		List<Element> writerEs = (List<Element>) document.selectNodes(xpath
				+ "/writer");

		for (Element writerE : writerEs) {
			JobPluginConf writerPluginConf = new JobPluginConf();

			Element writerPluginE = (Element) writerE
					.selectSingleNode("plugin");
			String writerName = writerPluginE.getStringValue().trim()
					.toLowerCase();
			String writerId = writerE.attributeValue("id");
			writerPluginConf.setPluginName(writerName);
			writerPluginConf.setId(writerId == null ? "writer-id-"
					+ writerEs.indexOf(writerE) + "-" + writerName : writerId
					.trim());

			Map<String, String> writerPluginParamMap = getParamMap(writerE);

			IParam writerPluginParam = new DefaultParam(writerPluginParamMap);

			writerPluginConf.setPluginParam(writerPluginParam);
			writerJobConfs.add(writerPluginConf);
		}

		job.setReaderConf(readerJobConf);
		job.setWriterConfs(writerJobConfs);

		return job;
	}

	/**
	 * Parse plugin configure file.
	 * 
	 * @return a map mapping plugin name to plugin params
	 */
	@SuppressWarnings("unchecked")
	public static Map<String, IParam> loadPluginConf() {
		File f = new File(Environment.PLUGINS_CONF);
		Document doc = null;

		try {
			String xml = FileUtils.readFileToString(f);
			doc = DocumentHelper.parseText(xml);
		} catch (IOException e) {
			LOG.error(e.getMessage());
		} catch (DocumentException e) {
			LOG.error("the document could not be parsed. " + e.getMessage());
		}

		Map<String, IParam> pluginsMap = new HashMap<String, IParam>();
		String xpath = "/plugins/plugin";
		List<Element> pluginsEs = (List<Element>) doc.selectNodes(xpath);
		for (Element pluginsE : pluginsEs) {
			Map<String, String> pluginParamsMap = getParamMap(pluginsE);

			if (pluginParamsMap.containsKey("name")) {
				IParam plugin = new DefaultParam(pluginParamsMap);
				pluginsMap.put(pluginParamsMap.get("name"), plugin);
			} else {
				LOG.error(String
						.format("WormHole plugin configure file can't find xpath \"%s\" plugin name",
								pluginsE.getPath()));
			}
		}

		return pluginsMap;
	}

	/**
	 * Parse engine configuration file.
	 * 
	 * @return {@link EngineConf}.
	 * 
	 * */
	public static IParam loadEngineConfig() {
		File file = new File(Environment.ENGINE_CONF);

		Document doc = null;
		try {
			String xml = FileUtils.readFileToString(file);
			doc = DocumentHelper.parseText(xml);
		} catch (IOException e) {
			LOG.error("WormHole can not find engine.xml .");
		} catch (DocumentException e) {
			LOG.error("WormHole can not parse engine.xml .");
		}

		String xpath = "/engine";
		Element engineE = (Element) doc.selectSingleNode(xpath);
		Map<String, String> engineParam = getParamMap(engineE);

		return new DefaultParam(engineParam);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, String> getParamMap(Element rootElement) {
		Map<String, String> paramMap = new HashMap<String, String>();
		List<Element> paramsEs = (List<Element>) rootElement.selectNodes("./*");
		for (Element paramsE : paramsEs) {
			paramMap.put(paramsE.getName().trim(), paramsE.getStringValue());
		}
		return paramMap;
	}

}
