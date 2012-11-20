package com.dp.nebula.wormhole.plugins.reader.sftpreader;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IReader;
import com.dp.nebula.wormhole.plugins.common.PCInfo;
import com.dp.nebula.wormhole.plugins.common.SFTPUtils;
import com.dp.nebula.wormhole.plugins.reader.sftpreader.ParamKey;
import com.hadoop.compression.lzo.LzopCodec;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class SftpReader extends AbstractPlugin implements IReader {
	private static final Logger logger = Logger.getLogger(SftpReader.class);
	public static final int LINE_MAX_FIELD = 1024;
	
	private char fieldSplit = '\t';
	private String encoding = "UTF-8";
	private int bufferSize = 4 * 1024;
	private String colFilter = "";
	private String nullString = "";
	private String dir = "";
	private String firstLineReadOrNot = "true";
	private String fileType = "txt";
	
	private int[] colList = new int[LINE_MAX_FIELD];
	private Map<Integer, String> constColMap = new HashMap<Integer, String>();
	private boolean colListSwitch = false;
	
	private URI uri = null;
	private String username = "";
	private String password = "";
	private String host = "";
	private int port = 58422;
	private String path = "";
	
	private JSch jsch = null;
	private Session session = null;
	private Channel channel = null;
	private ChannelSftp c = null;
	
	private GZIPInputStream gin = null;
	private CompressionInputStream cin = null;
	private InputStream in = null;
	private BufferedReader br = null;
	
	@Override
	public void init() {
		bufferSize = getParam().getIntValue(ParamKey.bufferSize, this.bufferSize);
		fieldSplit = getParam().getCharValue(ParamKey.fieldSplit, this.fieldSplit);
		encoding = getParam().getValue(ParamKey.encoding, this.encoding);
		nullString = getParam().getValue(ParamKey.nullString, this.nullString);
		colFilter = getParam().getValue(ParamKey.colFilter.trim(), this.colFilter);
		dir = getParam().getValue(ParamKey.dir, this.dir);
		fileType = getParam().getValue(ParamKey.fileType, this.fileType);
		firstLineReadOrNot = getParam().getValue(ParamKey.firstLineReadOrNot, this.firstLineReadOrNot);
		
		if (StringUtils.isBlank(dir)) {
			logger.error("Can't find the param ["
					+ ParamKey.dir + "] in hdfs-reader-param.");
			return;
		}
		
		if (!StringUtils.isBlank(colFilter)) {
			for (int i = 0; i < colList.length; ++i) {
				colList[i] = -1;
			}
			String[] cols = colFilter.split(",");
			for (int index = 0; index < cols.length; ++index) {
				String filter = cols[index].trim();
				if (filter.startsWith("#")) {
					try {
						int colIndex = Integer.valueOf(filter.substring(1));
						if (colIndex >= colList.length) {
							logger.error(String.format("Columns index larger than %d, not supported .",
									LINE_MAX_FIELD));
							return;
						}
						colList[colIndex] = index;
					} catch (NumberFormatException e) {
						logger.error(e.getCause());
						return;
					}
				} else if ("null".equalsIgnoreCase(filter)) {
					constColMap.put(index, "");
				} else {
					constColMap.put(index, filter);
				}
			}
			if (cols.length > 0) {
				colListSwitch = true;
			}
		}
		
		uri = URI.create(dir);
		String scheme = uri.getScheme();
		logger.debug("sftp reader uri:" + uri.toString());
		if (!scheme.equalsIgnoreCase("sftp")) {
			logger.error("Sftp path missing scheme, check path begin with sftp:// .");
			return;
		}
		
		username = uri.getUserInfo();
		password = getParam().getValue(ParamKey.password, this.password);
		host = uri.getHost();
		port = uri.getPort();
		path = uri.getPath();
	}
	
	@Override
	public void connection() {
		PCInfo pi = new PCInfo();
		pi.setIp(this.host);
		pi.setPort(this.port);
		pi.setUser(this.username);
		pi.setPwd(this.password);
		pi.setPath(this.path);
		
		try {
			jsch = new JSch();
			session = jsch.getSession(username, host, port);
			session.setUserInfo(pi);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			c = (ChannelSftp) channel;
			in = c.get(path);
			
			if (fileType.equalsIgnoreCase("txt")){
				br = new BufferedReader(
                        new InputStreamReader(in, encoding), bufferSize);
			}else if (fileType.equalsIgnoreCase("gz") || fileType.equalsIgnoreCase("gzip")){
				gin = new GZIPInputStream(in);
	            br = new BufferedReader(
	                    new InputStreamReader(gin, encoding), bufferSize);
			}else if (fileType.equalsIgnoreCase("lzo")){
				LzopCodec lzopCodec = new LzopCodec();
				lzopCodec.setConf(SFTPUtils.getConf());
				
				cin = lzopCodec.createInputStream(in);
	            br = new BufferedReader(new InputStreamReader(cin, encoding), bufferSize);
			}else{
				throw new IllegalArgumentException("illegal argument fileType=" + fileType);
			}
		} catch (Exception e){
			logger.error(e.getMessage());
			return;
		}
	}
	
	@Override
	public void read(ILineSender lineSender) {
		String lineString = null;
		try {
			if (firstLineReadOrNot.equalsIgnoreCase("false")){
				lineString = br.readLine();
				logger.info(String.format(
						"discard the first line: %s", lineString));
			}
			
			while ((lineString = br.readLine()) != null){
				ILine line = lineSender.createNewLine();
				int i = 0;
				int begin = 0;
				int length = lineString.length();
				if (!colListSwitch){
					for (i = 0; i < length; ++i) {
						if (lineString.charAt(i) == fieldSplit) {
							line.addField(replaceNullString(lineString.substring(begin, i)));
							begin = i + 1;
						}
					}
					line.addField(replaceNullString(lineString.substring(begin, i)));
				}else{
					int index = 0;
					for (i = 0; i < length; ++i) {
						if (lineString.charAt(i) == fieldSplit) {
							if (colList[index] >= 0){
								line.addField(replaceNullString(lineString.substring(begin, i)), 
										colList[index]);
							}
							begin = i + 1;
							index++;
						}
					}
					if (colList[index] >= 0) {
						line.addField(replaceNullString(lineString.substring(begin, i)),
								colList[index]);
					}
					
					// add constant columns
					for (Integer k : constColMap.keySet()) {
						line.addField(constColMap.get(k), k);
					}
				}
				logger.debug(line.toString(','));
				
				boolean flag = lineSender.send(line);
				if(getMonitor()!=null) {
					if (flag){
						getMonitor().increaseSuccessLines();
					}else{
						getMonitor().increaseFailedLines();
					}
				}
			}
		} catch (IOException e) {
			logger.error(e.toString());
		}
		lineSender.flush();
	}
	
	@Override
	public void finish() {
		try {
			closeAll();
		} catch (IOException e) {
			logger.error("close all error " + e.getMessage());
		}
	}
	
	private void closeAll() throws IOException{
		if (br != null){
			br.close();
		}
		
		if (cin != null){
			cin.close();
		}
		
		if (gin != null){
			gin.close();
		}
		
		if (in != null){
			in.close();
		}
		
		if (c != null){
			c.disconnect();
		}
		
		if (session != null){
			session.disconnect();
		}
	}
	
	private String replaceNullString(String string) {
		if (nullString != null && nullString.equals(string)) {
			return null;
		}
		return string;
	}
	
	public static void main(String[] args) throws IOException {
//		InputStream in = new FileInputStream("C:\\Users\\yukang.chen\\Desktop\\adf.txt.gz");
//		CompressionCodec codec = new GzipCodec();
//		CompressionInputStream cin = codec.createInputStream(in);
//		BufferedReader br = new BufferedReader(new InputStreamReader(cin, "utf-8"), 4 * 1024);
//		String line = "";
//		while ((line = br.readLine()) != null){
//			
//		}
	}
}
