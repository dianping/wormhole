package com.dp.nebula.wormhole.plugins.writer.hdfswriter;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.DFSUtils;

public class HdfsWriter extends AbstractPlugin implements IWriter {
	private static final Logger logger = Logger.getLogger(HdfsWriter.class);
	private static volatile boolean compressionTypePrintVirgin = true;
	
	private FileSystem fs;
	private Path p = null;
	private char fieldSplit = '\u0001';
	private char lineSplit = '\n';
	private int bufferSize = 8 * 1024;
	private String encoding = "UTF-8";
	private String dir = "";
	private String replaceChar = "";
	private Map<Character, Character> replaceCharMap = null;
	private char[] nullChars = null;
	
	private DfsWriterStrategy dfsWriterStrategy = null;
	
	static {
		Thread.currentThread().setContextClassLoader(HdfsWriter.class.getClassLoader());
	}
	
	@Override
	public void init() {
		fieldSplit = getParam().getCharValue(ParamKey.fieldSplit,
				fieldSplit);
		encoding = getParam().getValue(ParamKey.encoding, encoding);
		lineSplit = getParam().getCharValue(ParamKey.lineSplit,
				lineSplit);
		bufferSize = getParam().getIntValue(ParamKey.bufferSize,
				bufferSize);
		nullChars = getParam().getValue(ParamKey.nullChar, "")
				.toCharArray();
		replaceChar = getParam().getValue(ParamKey.replaceChar, "");
		
		replaceCharMap = parseReplaceChar(replaceChar);
		
		dir = getParam().getValue(ParamKey.dir, this.dir);

		try {
			fs = DFSUtils.createFileSystem(new URI(dir),
					DFSUtils.getConf(dir, null));
		} catch (Exception e) {
			logger.error(String.format(
					"HdfsWriter Initialize file system failed:%s,%s",
					e.getMessage(), e.getCause()));
			closeAll();
		}
	
		if (!StringUtils.isBlank(dir)) {
			p = new Path(dir);
		} else {
			closeAll();
			logger.error("Can't find the param ["
					+ ParamKey.dir + "] in hdfs-writer-param.");
			return;
		}

		String filetype = getParam().getValue(ParamKey.fileType, "TXT");
		if ("TXT_COMP".equalsIgnoreCase(filetype))
			dfsWriterStrategy = new DfsWriterTextFileStrategy(true);
		else if ("TXT".equalsIgnoreCase(filetype))
			dfsWriterStrategy = new DfsWriterTextFileStrategy(false);
		else {
			closeAll();
			logger.error(
					"HdfsWriter cannot recognize filetype: " + filetype);
		}
	}

	private Map<Character, Character> parseReplaceChar(String replaceChar) {
		replaceCharMap = new HashMap<Character, Character>();
		if (!StringUtils.isBlank(replaceChar)){
			String[] items = StringUtils.split(replaceChar, ":");
			if (2 == items.length){
				char[] srcChars = items[0].toCharArray();
				char[] destChars = items[1].toCharArray();
				if (destChars.length != 1 || srcChars.length == 0){
					throw new IllegalArgumentException(String.format(
							"paramKey replaceChar '%s' are not properly set, pleace check it again", replaceChar));
				}else{
					for (char srcC : srcChars)
						replaceCharMap.put(srcC, destChars[0]);
				}
			}
		}
		return replaceCharMap;
	}

	@Override
	public void connection() {
		if (p == null) {
			closeAll();
			logger.error("HdfsWriter Can't initialize file system .");
		}
		try {
			dfsWriterStrategy.open();
		} catch (Exception ex) {
			closeAll();
			logger.error(ex.toString());
		}
	}

	@Override
	public void finish() {
		closeAll();
	}

	@Override
	public void commit() {
	}

	@Override
	public void write(ILineReceiver lineReceiver) {
		try {
			dfsWriterStrategy.write(lineReceiver);
		} catch (Exception ex) {
			logger.error(String.format(
					"Some errors occurs on starting writing: %s,%s",
					ex.getMessage(), ex.getCause()));
		} finally {
			dfsWriterStrategy.close();
			closeAll();
		}

	}

	public interface DfsWriterStrategy {
		void open();

		void write(ILineReceiver receiver);

		void close();
	}
	
	class DfsWriterTextFileStrategy implements DfsWriterStrategy {
		private FSDataOutputStream out = null;

		private BufferedWriter bw = null;

		private CompressionOutputStream co = null;

		private boolean compressed = false;

		public DfsWriterTextFileStrategy(boolean compressed) {
			this.compressed = compressed;
		}

		@Override
		public void close() {
			IOUtils.cleanup(null, bw, out, co);
		}

		@Override
		public void open() {
			try {
				if (compressed) {
					logger.info("creating compressed file " + p.toString());
					
					//using LzopCodec as default option
					String codecClassName = getParam().getValue(
							ParamKey.codecClass,
							"com.hadoop.compression.lzo.LzopCodec");

					Class<?> codecClass = Class.forName(codecClassName);
					Configuration conf = DFSUtils.getConf(dir, null);
					CompressionCodec codec = (CompressionCodec) ReflectionUtils
							.newInstance(codecClass, conf);
					
					if (compressionTypePrintVirgin) {
						logger.info("reflection using compression codec class: " + codec.getClass().getName());
						compressionTypePrintVirgin = false;
					}
					
					out = fs.create(p, false, bufferSize);
					co = codec.createOutputStream(out);
					bw = new BufferedWriter(
							new OutputStreamWriter(co, encoding), bufferSize);
				} else {
					out = fs.create(p, false, bufferSize);
					bw = new BufferedWriter(new OutputStreamWriter(out,
							encoding), bufferSize);
				}
			} catch (Exception e) {
				logger.error(e.toString());
			}
		}

		@Override
		public void write(ILineReceiver receiver) {
			ILine line;
			try {
				while ((line = receiver.receive()) != null) {
					int len = line.getFieldNum();
					for (int i = 0; i < len; i++) {
						bw.write(replaceChars(line.getField(i), replaceCharMap));
						if (i < len - 1)
							bw.write(fieldSplit);
					}
					bw.write(lineSplit);
					
					getMonitor().increaseSuccessLines();
				}
				bw.flush();
			} catch (Exception e) {
				logger.error(e.toString(),e);
			}
		}
	}
	
	/**
	 * Replace field string with space character when it contains field split character, \r or \n
	 * 
	 * @param str
	 *            source string
	 * 
	 * @param fieldSplit   
	 *   		  fieldSplit
	 * @return replaced character array.
	 * */
	private char[] replaceChars(String str, Map<Character, Character> replaceCharMap) {
		if (null == str) {
			return this.nullChars;
		}
		
		char[] newchars = str.toCharArray();
		int strLength = newchars.length;
		
		//when user doesn't fill the replaceChar parameter, we will replace \r \n and fieldSplit with ' ' as default
		if (replaceCharMap == null || replaceCharMap.isEmpty()){
			for (int i = 0; i < strLength; i++) {
				if (fieldSplit == newchars[i] || 13 == newchars[i]
						|| 10 == newchars[i]) {
					newchars[i] = ' ';
				}	
			}
		}
		// else we will replace char as user specify 
		else{
			for (int i = 0; i < strLength; i++) {
				if (replaceCharMap.containsKey(newchars[i])){
					newchars[i] = replaceCharMap.get(newchars[i]);
				}
			}
		}
		
		return newchars;
	}
	
	private void closeAll() {
		try {
			IOUtils.closeStream(fs);
		} catch (Exception e) {
			logger.error(String.format(
					"HdfsWriter closing filesystem failed: %s,%s",
					e.getMessage(), e.getCause()));
		}
	}
	
	public static void main(String[] args) {
		HdfsWriter hw = new HdfsWriter();
		Map<Character, Character> testCharMap = hw.parseReplaceChar("\r\n\t:\001");
		for (Character c : testCharMap.keySet()) {
			logger.debug(c + ":" + testCharMap.get(c));
		}
	}
}
