package com.dp.nebula.wormhole.plugins.reader.hdfsreader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IReader;
import com.dp.nebula.wormhole.plugins.common.DFSUtils;
import com.dp.nebula.wormhole.plugins.common.DFSUtils.HdfsFileType;

public class HdfsReader extends AbstractPlugin implements IReader {
	private static final Logger logger = Logger.getLogger(HdfsReader.class);
	
	public static final int LINE_MAX_FIELD = 1024;
	private static volatile boolean fileTypePrintVirgin = true;
	
	private char fieldSplit = '\t';
	private String encoding = "UTF-8";
	private int bufferSize = 4 * 1024;
	private String colFilter = null;
	private String nullString = "";
	private String dir = null;
	private String firstLineReadSwitch = "true";
	
	private int[] colList = new int[LINE_MAX_FIELD];
	private Map<Integer, String> constColMap = new HashMap<Integer, String>();
	private boolean colListSet = false;
	
	private Path p = null;
	private int emptyFile = 0;
	
	private FileSystem fs = null;
	private DfsReaderStrategy readerStrategy = null;
	private static Map<DFSUtils.HdfsFileType, Class<? extends DfsReaderStrategy>> readerStrategyMap = null;
	
	static {
		readerStrategyMap = new HashMap<DFSUtils.HdfsFileType, Class<? extends DfsReaderStrategy>>();
		readerStrategyMap.put(DFSUtils.HdfsFileType.TXT,
				DfsReaderTextFileStrategy.class);
		readerStrategyMap.put(DFSUtils.HdfsFileType.COMP_TXT,
				DfsReaderCompTextFileStrategy.class);
		
		Thread.currentThread().setContextClassLoader(
				HdfsReader.class.getClassLoader());
	}
	
	@Override
	public void read(ILineSender lineSender) {
		try {
			if (emptyFile > -1) {
				readerStrategy.registerSender(lineSender);
				
				/* discard the first line if user set firstLineReadSwitch to false*/
				if (firstLineReadSwitch.equalsIgnoreCase("false")){
					readerStrategy.next();
					logger.info(String.format(
							"discard the first line: %s", readerStrategy.getLineString()));
				}
				
				while (readerStrategy.next()) {
					readerStrategy.sendToWriter();
				}
				lineSender.flush();
			}
		} catch (Exception ex) {
			logger.error(String.format(
					"Errors in starting hdfsreader: %s, %s", ex.getMessage(),
					ex.getCause()));
		} finally {
			readerStrategy.close();
			closeAll();
		}
	}

	@Override
	public void connection() {
		try {
			Configuration conf = DFSUtils.getConf(dir, null);
			HdfsFileType fileType = DFSUtils.checkFileType(fs, new Path(dir), conf);
			Class<? extends DfsReaderStrategy> recogniser = readerStrategyMap.get(fileType);
			String name = recogniser.getName().substring(
					recogniser.getName().lastIndexOf(".") + 1);
			if (fileTypePrintVirgin) {
				logger.info(String.format("Recognise filetype, use %s .", name));
				fileTypePrintVirgin = false;
			}
			readerStrategy = (DfsReaderStrategy) recogniser.getConstructors()[0]
					.newInstance(this);
		} catch (Exception e) {
			logger.error(e.getMessage());
			closeAll();
			return;
		}
		
		p = new Path(dir);

        try {
			if (!fs.exists(p)) {
				closeAll();
				logger.error("File [" + dir
						+ "] does not exist.");
				return;
			}

			emptyFile = readerStrategy.open();
			return;
		} catch (IOException e) {
			closeAll();
			logger.error(String.format(
					"Initialize file system is failed:%s,%s", e.getMessage(),
					e.getCause()));
		}
	}

	@Override
	public void finish() {
		closeAll();
	}

	// need to be added
	@Override
	public Map<String, String> getMonitorInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void init() {
		bufferSize = getParam().getIntValue(ParamKey.bufferSize,
				bufferSize);
		fieldSplit = getParam().getCharValue(ParamKey.fieldSplit, '\t');
		encoding = getParam().getValue(ParamKey.encoding, "UTF-8");
		nullString = getParam().getValue(ParamKey.nullString,
				this.nullString);
		colFilter = getParam().getValue(ParamKey.colFilter, "");
		dir = getParam().getValue(ParamKey.dir, "");
		firstLineReadSwitch = getParam().getValue(ParamKey.firstLineReadSwitch, this.firstLineReadSwitch);
		
		/* check parameters */
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
					}
				} else if ("null".equalsIgnoreCase(filter)) {
					constColMap.put(index, "");
				} else {
					constColMap.put(index, filter);
				}
			}
			if (cols.length > 0) {
				colListSet = true;
			}
		}
		
		/* check hdfs file type */
		try {
			Configuration conf = DFSUtils.getConf(dir, null);
			fs = DFSUtils.createFileSystem(new URI(dir), conf);
		} catch (Exception e) {
			closeAll();
			logger.error(String.format(
					"Initialize file system failed:%s,%s", e.getMessage(),
					e.getCause()));
		}

		if (fs == null) {
			closeAll();
			logger.error("Create FileSystem failed");
		}
	}
	
	private void closeAll(){
		try {
			IOUtils.closeQuietly(fs);			
		}catch (Exception e) {
			logger.error(String.format(
					"Hdfs closing failed:%s, s", e.getMessage(),
					e.getCause()));
		}
	}
	
	public interface DfsReaderStrategy {
		int open() throws IOException;

		void registerSender(ILineSender sender);

		boolean next() throws IOException;

		ILine sendToWriter();
		
		String getLineString();

		void close();
	}
	
	class DfsReaderTextFileStrategy implements DfsReaderStrategy{
		
		private Configuration conf = null;
		private FSDataInputStream in = null;
		private CompressionInputStream cin = null;
		private BufferedReader br = null;
		private ILineSender sender = null;
		private String s = null;
		private boolean compressed = false;
		
		private DfsReaderTextFileStrategy(boolean compressed){
			this.compressed = compressed;
			try {
				this.conf = DFSUtils.getConf(dir, null);
			} catch (IOException e) {
				logger.error("some error occur when get configuration :" + e.getMessage());
			}
		}
		
		public DfsReaderTextFileStrategy(){
			this(false);
		}

		@Override
		public void close() {
			IOUtils.closeQuietly(br);
			IOUtils.closeQuietly(cin);
			IOUtils.closeQuietly(in);
		}

		@Override
		public boolean next() throws IOException {
            s = br.readLine();
            if (null != s) {
				return true;
            }
			return false;
		}
		
		@Override
		public String getLineString(){
			if (null != s) {
				return s;
			}
			return "Null LineString";
		}

		@Override
		public int open() throws IOException {
			if (compressed) {
                CompressionCodecFactory factory = new CompressionCodecFactory(
                        conf);
                CompressionCodec codec = factory.getCodec(p);
                if (codec == null) {
                    throw new IOException(
                            String.format(
                                    "Can't find any suitable CompressionCodec to this file:%s",
                                    p.toString()));
                }
                in = fs.open(p);
                
                //System.setProperty("LD_LIBRARY_PATH", "/usr/local/hadoop/lzo/lib");
                cin = codec.createInputStream(in);
                br = new BufferedReader(
                        new InputStreamReader(cin, encoding), bufferSize);
            } else {
                in = fs.open(p);
                br = new BufferedReader(
                        new InputStreamReader(in, encoding), bufferSize);
            }
            if (in.available() == 0) {
                return -1;
            } else {
                return 0;
            }
		}

		@Override
		public void registerSender(ILineSender sender) {
			this.sender = sender;
		}

		@Override
		public ILine sendToWriter() {
			if (null == sender) {
				throw new IllegalStateException("LineSender cannot be null .");
			}

			ILine line = sender.createNewLine();
			int begin = 0;
			int i = 0;
			if (!colListSet) {
				for (i = 0; i < s.length(); ++i) {
					if (s.charAt(i) == fieldSplit) {
						line.addField(replace(s.substring(begin, i)));
						begin = i + 1;
					}
				}
				// last field
				line.addField(replace(s.substring(begin, i)));
			} else {
				int colIndex = 0;
				for (i = 0; i < s.length(); ++i) {
					if (s.charAt(i) == fieldSplit) {
						if (colList[colIndex] >= 0) {
							line.addField(replace(s.substring(begin, i)),
									colList[colIndex]);
						}
						begin = i + 1;
						colIndex++;
					}
				}
				if (colList[colIndex] >= 0) {
					line.addField(replace(s.substring(begin, i)),
							colList[colIndex]);
				}
				// add constant columns
				for (Integer k : constColMap.keySet()) {
					line.addField(constColMap.get(k), k);
				}
			}
			boolean flag = sender.send(line);

			if (flag) {
				getMonitor().increaseSuccessLines();
			} else {
				getMonitor().increaseFailedLines();
			}
			return line;
		}

		/**
		 * @param string
		 * 				field string
		 * @return 
		 * 		  		if the field string is equal to specified nullString, it return null
		 */
		private String replace(String string) {
			if (nullString != null && nullString.equals(string)) {
				return null;
			}
			return string;
		}
		
	}
	
	class DfsReaderCompTextFileStrategy extends DfsReaderTextFileStrategy{
		public DfsReaderCompTextFileStrategy() {
			super(true);
		}	
	}
}
