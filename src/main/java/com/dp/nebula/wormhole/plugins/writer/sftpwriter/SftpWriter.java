package com.dp.nebula.wormhole.plugins.writer.sftpwriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.PCInfo;
import com.dp.nebula.wormhole.plugins.common.SFTPUtils;
import com.hadoop.compression.lzo.LzopCodec;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

public class SftpWriter extends AbstractPlugin implements IWriter {
	private static final Logger logger = Logger
			.getLogger(SftpWriter.class);

	private String dir = "";
	private String host = "";
	private int port = 58422;
	private String path = "";
	private String username = "";
	private String password = "";
	private URI uri = null;
	private String fileType = "txt";
	private char fieldSplit = '\t';
	private char lineSplit = '\n';
	private String encoding = "UTF-8";
	private int bufferSize = 4 * 1024;
	private char[] nullChars = null;

	private JSch jsch = null;
	private Session session = null;
	private Channel channel = null;
	private ChannelSftp c = null;

	private GZIPOutputStream gout = null;
	private CompressionOutputStream cout = null;
	private OutputStream out = null;
	private BufferedWriter bw = null;

	@Override
	public void commit() {
	}

	@Override
	public void init() {
		dir = getParam().getValue(ParamKey.dir, this.dir).trim();
		if (StringUtils.isBlank(dir)) {
			logger.error("Can't find the param [" + ParamKey.dir
					+ "] in sftp-spliter-param.");
			return;
		}
		fileType = getParam().getValue(ParamKey.fileType, this.fileType).trim();
		bufferSize = getParam().getIntValue(ParamKey.bufferSize,
				this.bufferSize);
		fieldSplit = getParam().getCharValue(ParamKey.fieldSplit,
				this.fieldSplit);
		lineSplit = getParam().getCharValue(ParamKey.lineSplit, this.lineSplit);
		encoding = getParam().getValue(ParamKey.encoding, this.encoding);
		nullChars = getParam().getValue(ParamKey.nullChar, "").toCharArray();
	}

	@Override
	public void connection() {
		uri = URI.create(dir);
		host = uri.getHost();
		port = uri.getPort();
		path = uri.getPath();
		username = uri.getUserInfo();
		password = getParam().getValue(ParamKey.password, this.password);

		PCInfo pi = new PCInfo();
		pi.setIp(host);
		pi.setPort(port);
		pi.setUser(username);
		pi.setPwd(password);
		pi.setPath(path);

		try {
			jsch = new JSch();
			session = jsch.getSession(username, host, port);
			session.setUserInfo(pi);
			session.connect();
			channel = session.openChannel("sftp");
			channel.connect();
			c = (ChannelSftp) channel;
			out = c.put(path);

			if (fileType.equalsIgnoreCase("txt")) {
				bw = new BufferedWriter(new OutputStreamWriter(out, encoding),
						bufferSize);
			} else if (fileType.equalsIgnoreCase("gz")
					|| fileType.equalsIgnoreCase("gzip")) {
				gout = new GZIPOutputStream(out);
				bw = new BufferedWriter(new OutputStreamWriter(gout, encoding),
						bufferSize);
			} else if (fileType.equalsIgnoreCase("lzo")) {
				LzopCodec lzopCodec = new LzopCodec();
				lzopCodec.setConf(SFTPUtils.getConf());

				cout = lzopCodec.createOutputStream(out);
				bw = new BufferedWriter(new OutputStreamWriter(cout, encoding),
						bufferSize);
			} else {
				throw new IllegalArgumentException("illegal argument fileType="
						+ fileType);
			}
		} catch (Exception e) {
			closeAll();
			throw new RuntimeException("something wrong with jsch:"
					+ e.getMessage());
		}
	}

	@Override
	public void finish() {
		closeAll();
	}

	@Override
	public void write(ILineReceiver lineReceiver) {
		ILine line = null;
		try {
			while ((line = lineReceiver.receive()) != null) {
				int len = line.getFieldNum();
				for (int i = 0; i < len; i++) {
					bw.write(replaceChars(line.getField(i)));
					if (i < len - 1) {
						bw.write(fieldSplit);
					}
				}
				bw.write(lineSplit);
				if(getMonitor()!=null) {
					getMonitor().increaseSuccessLines();
				}
			}
			bw.flush();
		} catch (Exception e) {
			logger.error(e.toString());
		}
	}

	private char[] replaceChars(String str) {
		if (null == str) {
			return this.nullChars;
		}
		return str.toCharArray();
	}

	private void closeAll() {
		try {
			if (bw != null) {
				bw.close();
			}
			if (cout != null) {
				cout.close();
			}
			if (gout != null) {
				gout.close();
			}
			if (out != null) {
				out.close();
			}
			if (c != null) {
				c.disconnect();
			}
			if (session != null) {
				session.disconnect();
			}
		} catch (IOException e) {
			logger.error(e.toString());
		}
	}
}
