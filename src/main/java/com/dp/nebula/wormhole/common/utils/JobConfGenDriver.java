package com.dp.nebula.wormhole.common.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;

public final class JobConfGenDriver {
	private static final Log logger = LogFactory.getLog(JobConfGenDriver.class.getName());
	
	private static final String METHOD_DECLARATION = "(/\\*(.+?)\\*/)([^;]+;)";
	private static final String COMMENT_PATTERN = "(\\w+\\s*)(:)([^\\r\\n]+)";
	private static final String MEMBER_PATTERN = "([^=]=\\s*)(\"[^\"]+\")";
	
	public enum PluginType {
		Reader, Writer;
	}
	
	// Suppress default constructor for non-instantiability
	private JobConfGenDriver(){
		throw new AssertionError();
	}
	
	public static void showCopyRight() {
		System.out.println("Welcome to using DianPing WormHole Version 0.1");
	}
	
	public static List<String> getPluginDirAsList(String pluginsDirName) {
		List<String> pluginsList = new ArrayList<String>();
		String lastNameOfPluginsDirName = pluginsDirName.substring(pluginsDirName.lastIndexOf(File.separator) + 1);
		for (File file : new File(pluginsDirName).listFiles()) {
			if (file.isDirectory() && file.getName().endsWith(lastNameOfPluginsDirName)) {
				pluginsList.add(file.getName());
			}
		}
		Collections.sort(pluginsList);
		return pluginsList;
	}
	
	public static int showPluginsInfo(String prefixInfo, List<String> pluginsList) {
		int pluginsSize = pluginsList.size();
		int choice = -1;
		while(choice  < 0 || choice > pluginsSize - 1) {
			System.out.println(prefixInfo);
			
			for (int idx = 0; idx < pluginsSize; idx++) {
				System.out
					.println(String.format("%d\t%s",
						idx,
						pluginsList.get(idx).toLowerCase().replace("reader", "")
								.replace("writer", "")));
			}
			
			System.out.print(String.format("Please choose [%d-%d]: ", 0,
					pluginsSize - 1));
			try {
				choice = Integer.parseInt(new Scanner(System.in).nextLine());
			}catch (NumberFormatException e) {
				System.out.println("Your chosen number is not correct. Please choose it again.");
				choice = -1;
			}
		}
		return choice;
	}
	
	public static void generateJobConfXml() throws IOException {
		showCopyRight();
		
		Map<PluginType, List<String>> pluginMap = new HashMap<PluginType, List<String>>();
		
		pluginMap.put(PluginType.Reader, getPluginDirAsList(Environment.READER_PLUGINS_DIR));
		pluginMap.put(PluginType.Writer, getPluginDirAsList(Environment.WRITER_PLUGINS_DIR));
		
		int readerPluginIndex = showPluginsInfo("WormHole Data Source: ", pluginMap.get(PluginType.Reader));
		int writerPluginIndex = showPluginsInfo("WormHole Data Destination: ", pluginMap.get(PluginType.Writer));
		
		String readerPluginName = pluginMap.get(PluginType.Reader).get(readerPluginIndex);
		String writerPluginName = pluginMap.get(PluginType.Writer).get(writerPluginIndex);
		String readerPluginPath = String.format("%s/%s/ParamKey.java", 
				Environment.READER_PLUGINS_DIR, readerPluginName);
		String writerPluginPath = String.format("%s/%s/ParamKey.java", 
				Environment.WRITER_PLUGINS_DIR, writerPluginName);
		
		ClassNode readerClassNode = parse(readerPluginName, readerPluginPath);
		ClassNode writerClassNode = parse(writerPluginName, writerPluginPath);
		
		String jobFileName = MessageFormat.format("wormhole_{0}_to_{1}_{2}.xml", 
				readerClassNode.getName(),
					writerClassNode.getName(),
						System.currentTimeMillis());
		int retStatus = generateXmlFile(readerClassNode, writerClassNode, jobFileName);
		if (0 == retStatus) {
			System.out.println(String.format("Create jobfile %s completed.", jobFileName));
		}
	}
	
	public static int generateXmlFile(ClassNode readerClassNode, ClassNode writerClassNode, String fileName) 
			throws IOException {
		Document doc = DocumentHelper.createDocument();
		Element jobE = doc.addElement("job");
		String jobId = readerClassNode.getName() + "_to_" + writerClassNode.getName() + "_job";
		jobE.addAttribute("id", jobId);
		
		/* add reader part */
		Element readerE = jobE.addElement("reader");
		Element pluginE = readerE.addElement("plugin");
		pluginE.setText(readerClassNode.getName());
		
		Element tempElement = null;

		List<ClassMember> members = readerClassNode.getAllMembers();
		for (ClassMember member : members) {
			StringBuilder command = new StringBuilder("\n");

			Set<String> set = member.getAllKeys();
			String value = "";
			for (String key : set) {
				value = member.getAttr("default");
				command.append(key)
						.append(":")
							.append(member.getAttr(key))
								.append("\n");
			}
			readerE.addComment(command.toString());

			String keyName = member.getName();
			keyName = keyName.substring(1, keyName.length() - 1);
			tempElement = readerE.addElement(keyName);

			if (StringUtils.isEmpty(value)) {
				value = "";
			}
			tempElement.setText(value);
		}
		
		/* add writer part */
		Element writerE = jobE.addElement("writer");
		pluginE = writerE.addElement("plugin");
		pluginE.setText(writerClassNode.getName());
		
		members = writerClassNode.getAllMembers();
		for (ClassMember member : members) {
			StringBuilder command = new StringBuilder("\n");
			
			Set<String> set = member.getAllKeys();
			String value = "";
			for (String key : set) {
				value = member.getAttr("default");
				command.append(key)
						.append(":").
							append(member.getAttr(key))
								.append("\n");
			}
			writerE.addComment(command.toString());

			String keyName = member.getName();
			keyName = keyName.substring(1, keyName.length() - 1);
			tempElement = writerE.addElement(keyName);

			if (!StringUtils.isBlank(value)) {
				tempElement.addText(value);
			}else{
				tempElement.addText("");
			}
		}
		
		XMLWriter output;
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setEncoding("UTF-8");
        output = new XMLWriter(new FileWriter(fileName), format);
        output.write(doc);
        output.close();
        
        return 0;
	}
	
	public static ClassNode parse(String name, String path) {
		String source = "";
		try {
			source = FileUtils.readFileToString(new File(path));
		} catch (IOException e) {
			logger.error(org.apache.hadoop.util.StringUtils.stringifyException(e));
			return null;
		}
		
		source = source.substring(source.indexOf('{') + 1);
		source = source.substring(0, source.lastIndexOf('}'));
		
		ClassNode node = ClassNode.newInstance();
		node.setName(name);

		Pattern pattern = Pattern.compile(METHOD_DECLARATION, Pattern.DOTALL);
		Matcher matcher = pattern.matcher(source);

		while (matcher.find()) {
			/* parse comment */
			Pattern commentPattern = Pattern.compile(COMMENT_PATTERN);
			Matcher commentMatcher = commentPattern.matcher(matcher.group(1));
			if (!commentMatcher.find()) {
				throw new IllegalArgumentException(
						"File format error: class declaration without comment @"
								+ matcher.group(1));
			}

			Map<String, String> attributes = new HashMap<String, String>();
			do {
				attributes.put(commentMatcher.group(1), commentMatcher.group(3)
						.trim());
			} while (commentMatcher.find());

			/* add key */
			Pattern memberPattern = Pattern.compile(MEMBER_PATTERN);
			Matcher memberMatcher = memberPattern.matcher(matcher.group(3));

			if (!memberMatcher.find()) {
				throw new IllegalArgumentException(
						"File format error: comment without member declaration @"
								+ matcher.group(3));
			}
			node.addMember(ClassNode.createMember(memberMatcher.group(2), attributes));
		}

		return node;
	}
}
