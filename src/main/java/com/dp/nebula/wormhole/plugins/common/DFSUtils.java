package com.dp.nebula.wormhole.plugins.common;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

public final class DFSUtils {
	private static final Logger LOGGER = Logger.getLogger(DFSUtils.class);

	private static ThreadLocal<FileSystem> fs;

	private static Map<String, Class<?>> typeMap = null;

	private static Map<String, String> compressionSuffixMap = null;

	static {
		typeMap = new HashMap<String, Class<?>>();
		typeMap.put("org.apache.hadoop.io.BooleanWritable", boolean.class);
		typeMap.put("org.apache.hadoop.io.ByteWritable", byte.class);
		typeMap.put("org.apache.hadoop.io.IntWritable", int.class);
		typeMap.put("org.apache.hadoop.io.VIntWritable", int.class);
		typeMap.put("org.apache.hadoop.io.LongWritable", long.class);
		typeMap.put("org.apache.hadoop.io.VLongWritable", long.class);
		typeMap.put("org.apache.hadoop.io.DoubleWritable", double.class);
		typeMap.put("org.apache.hadoop.io.FloatWritable", float.class);
		typeMap.put("org.apache.hadoop.io.Text", String.class);

		compressionSuffixMap = new HashMap<String, String>();
		compressionSuffixMap.put("org.apache.hadoop.io.compress.GzipCodec",
				"gz");
		compressionSuffixMap.put("org.apache.hadoop.io.compress.DefaultCodec",
				"deflate");
		compressionSuffixMap.put("com.hadoop.compression.lzo.LzopCodec", "lzo");
		compressionSuffixMap.put("org.apache.hadoop.io.compress.BZip2Codec",
				"bz2");
	}

	private DFSUtils() {
	}

	public static Map<String, Class<?>> getTypeMap() {
		return typeMap;
	}

	public static Map<String, String> getCompressionSuffixMap() {
		return compressionSuffixMap;
	}

	public enum HdfsFileType {
		TXT, COMP_TXT, SEQ,
	}

	// store configurations for per FileSystem schema
	private static Hashtable<String, Configuration> confs = new Hashtable<String, Configuration>();

	/**
	 * Get {@link Configuration}.
	 * 
	 * @param dir
	 *            directory path in hdfs
	 * 
	 * @param conf
	 *            hadoop-site.xml path
	 * 
	 * @return {@link Configuration}
	 * 
	 * @throws java.io.IOException
	 */
	public static Configuration getConf(String dir, String conf)
			throws IOException {
		URI uri = URI.create(dir);
		String scheme = uri.getScheme();
		if (null == scheme) {
			throw new IOException(
					"HDFS Path missing scheme, check path begin with hdfs://ip:port/ .");
		}

		Configuration cfg = confs.get(scheme);
		if (cfg == null) {
			cfg = new Configuration();
			cfg.setClassLoader(DFSUtils.class.getClassLoader());

			if (!StringUtils.isBlank(conf) && new File(conf).exists()) {
				LOGGER.info(String.format(
						"HdfsReader use %s for hadoop configuration .", conf));
				cfg.addResource(new Path(conf));
			}

			// System.setProperty("javax.xml.parsers.DocumentBuilderFactory",
			// "com.sun.org.apache.xerces.internal.jaxp.DocumentBuilderFactoryImpl");

			if (uri.getScheme() != null) {
				String fsname = String.format("%s://%s:%s", uri.getScheme(),
						uri.getHost(), uri.getPort());
				LOGGER.info("fs.default.name=" + fsname);
				cfg.set("fs.default.name", fsname);
			}

			cfg.set("io.compression.codecs",
					"org.apache.hadoop.io.compress.GzipCodec,"
							+ "org.apache.hadoop.io.compress.DefaultCodec,"
							+ "com.hadoop.compression.lzo.LzoCodec,"
							+ "com.hadoop.compression.lzo.LzopCodec,"
							+ "org.apache.hadoop.io.compress.BZip2Codec");

			cfg.set("fs." + scheme + ".impl.disable.cache", "true");
			cfg.set("hadoop.security.authentication", "kerberos");
			cfg.set("dfs.namenode.kerberos.principal",
					"hadoop/" + uri.getHost() + "@DIANPING.COM");

			confs.put(scheme, cfg);
		}
		return cfg;
	}

	/**
	 * Get one handle of {@link FileSystem}.
	 * 
	 * @param dir
	 *            directory path in hdfs
	 * 
	 * @param configure
	 *            hadoop-site.xml path
	 * 
	 * @return one handle of {@link FileSystem}.
	 * 
	 * @throws java.io.IOException
	 * 
	 * */

	public static FileSystem getFileSystem(String dir, String configure)
			throws IOException {
		if (fs.get() == null) {
			fs.set(FileSystem.get(getConf(dir, configure)));
		}
		return fs.get();
	}

	public static Configuration newConf() {
		Configuration conf = new Configuration();
		conf.setClassLoader(DFSUtils.class.getClassLoader());

		return conf;
	}

	/**
	 * Delete file specified by path or files in directory specified by path.
	 * 
	 * @param path
	 *            {@link Path} in hadoop
	 * 
	 * @param flag
	 *            need to do delete recursively
	 * 
	 * @param isGlob
	 *            need to use file pattern to match all files.
	 * 
	 * @throws IOException
	 * 
	 * */
	public static void deleteFiles(Path path, boolean flag, boolean isGlob)
			throws IOException {
		List<Path> paths = listDir(path, isGlob);
		for (Path p : paths) {
			deleteFile(p, flag);
		}
	}

	/**
	 * Delete file specified by path or files in directory specified by path.
	 * 
	 * @param dfs
	 *            handle of {@link FileSystem}
	 * 
	 * @param path
	 *            {@link Path} in hadoop
	 * 
	 * @param flag
	 *            need to do delete recursively
	 * 
	 * @param isGlob
	 *            need to use file pattern to match all files.
	 * 
	 * @throws IOException
	 * 
	 * */
	public static void deleteFiles(FileSystem dfs, Path path, boolean flag,
			boolean isGlob) throws IOException {
		List<Path> paths = listDir(dfs, path, isGlob);
		for (Path p : paths) {
			deleteFile(dfs, p, flag);
		}
	}

	/**
	 * List the statuses of the files/directories in the given path if the path
	 * is a directory.
	 * 
	 * @param srcpath
	 *            Path in {@link FileSystem}
	 * 
	 * @param isGlob
	 *            need to use file pattern
	 * 
	 * @return all {@link Path} in srcpath.
	 * @throws IOException
	 * 
	 * */
	public static List<Path> listDir(Path srcpath, boolean isGlob)
			throws IOException {
		List<Path> list = new ArrayList<Path>();
		FileStatus[] status = null;
		if (isGlob) {
			status = fs.get().globStatus(srcpath);
		} else {
			status = fs.get().listStatus(srcpath);
		}

		if (status != null) {
			for (FileStatus state : status) {
				list.add(state.getPath());
			}
		}

		return list;
	}

	/**
	 * List the statuses of the files/directories in the given path if the path
	 * is a directory.
	 * 
	 * @param dfs
	 *            handle of {@link FileSystem}
	 * 
	 * @param srcpath
	 *            Path in {@link FileSystem}
	 * 
	 * @param isGlob
	 *            need to use file pattern
	 * 
	 * @return all {@link Path} in srcpath
	 * 
	 * @throws IOException
	 * 
	 * */
	public static List<Path> listDir(FileSystem dfs, Path srcpath,
			boolean isGlob) throws IOException {
		List<Path> list = new ArrayList<Path>();
		FileStatus[] status = null;
		if (isGlob) {
			status = dfs.globStatus(srcpath);
		} else {
			status = dfs.listStatus(srcpath);
		}
		if (status != null) {
			for (FileStatus state : status) {
				list.add(state.getPath());
			}
		}

		return list;
	}

	/**
	 * Delete file specified by path.
	 * 
	 * @param path
	 *            {@link Path} in hadoop
	 * 
	 * @param flag
	 *            need to do delete recursively
	 * @throws IOException
	 * 
	 * */
	public static void deleteFile(Path path, boolean flag) throws IOException {
		LOGGER.debug("deleting:" + path.getName());
		fs.get().delete(path, flag);
	}

	/**
	 * Delete file specified by path.
	 * 
	 * @param dfs
	 *            handle of {@link FileSystem}
	 * 
	 * @param path
	 *            {@link Path} in hadoop
	 * 
	 * @param flag
	 *            need to do delete recursively
	 * @throws IOException
	 * 
	 * */
	public static void deleteFile(FileSystem dfs, Path path, boolean flag)
			throws IOException {
		LOGGER.debug("deleting:" + path.getName());
		dfs.delete(path, flag);
	}

	/**
	 * Initialize handle of {@link FileSystem}.
	 * 
	 * @param uri
	 *            URI
	 * 
	 * @param conf
	 *            {@link Configuration}
	 * 
	 * @return an FileSystem instance
	 */

	public static FileSystem createFileSystem(URI uri, Configuration conf)
			throws IOException {
		Class<?> clazz = conf.getClass("fs." + uri.getScheme() + ".impl", null);
		if (clazz == null) {
			throw new IOException("No FileSystem for scheme: "
					+ uri.getScheme());
		}
		conf.set("fs." + uri.getScheme() + ".impl.disable.cache", "true");
		UserGroupInformation.setConfiguration(conf);
		FileSystem fileSys = FileSystem.get(uri, conf);

		// String disableCacheName = String.format("fs.%s.impl.disable.cache",
		// uri.getScheme());
		// log.info("disableCache:" + conf.getBoolean(disableCacheName, false));
		// FileSystem fs = (FileSystem) ReflectionUtils.newInstance(clazz,
		// conf);
		// fs.initialize(uri, conf);
		return fileSys;
	}

	/**
	 * Check file type in hdfs.
	 * 
	 * @param fs
	 *            handle of {@link FileSystem}
	 * 
	 * @param path
	 *            hdfs {@link Path}
	 * 
	 * @param conf
	 *            {@link Configuration}
	 * 
	 * @return {@link HdfsFileType} TXT, TXT_COMP, SEQ
	 * */
	public static HdfsFileType checkFileType(FileSystem fs, Path path,
			Configuration conf) throws IOException {
		LOGGER.debug("io.compression.codecs:"
				+ conf.get("io.compression.codecs", "not found"));

		FSDataInputStream is = null;
		try {
			is = fs.open(path);
			/* file is empty, use TXT readerup */
			if (0 == is.available()) {
				return HdfsFileType.TXT;
			}

			switch (is.readShort()) {
			case 0x5345:
				if (is.readByte() == 'Q') {
					return HdfsFileType.SEQ;
				}
			default:
				is.seek(0);
				CompressionCodecFactory compressionCodecFactory = new CompressionCodecFactory(
						conf);
				CompressionCodec codec = compressionCodecFactory.getCodec(path);
				if (null == codec) {
					return HdfsFileType.TXT;
				} else {
					return HdfsFileType.COMP_TXT;
				}
			}
		} catch (IOException e) {
			throw e;
		} finally {
			if (null != is) {
				is.close();
			}
		}
	}
	public static void main(String []args) throws URISyntaxException{
		URI uri = new URI("hdfs://10.2.6.102/test");
		System.out.println(uri.getScheme() );
	}
}

	

