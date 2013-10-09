package com.dp.nebula.wormhole.plugins.reader.sftpreader;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.plugins.common.PCInfo;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;
import com.jcraft.jsch.SftpException;
import com.jcraft.jsch.ChannelSftp.LsEntry;

public class SftpDirSplitter extends AbstractSplitter {
	private static final Logger LOGGER = Logger
			.getLogger(SftpDirSplitter.class);
	private static final int UNDEFINED_PORT = -1;

	private String dir = "";
	private URI uri = null;
	private String scheme = "";
	private String host = "";
	private int port = 58422;
	private String path = "";
	private String username = "";
	private String password = "";
	private List<IParam> paramsList = null;

	private JSch jsch = null;
	private Session session = null;
	private Channel channel = null;
	private ChannelSftp c = null;

	@Override
	public void init(IParam jobParams) {
		super.init(jobParams);
		jsch = new JSch();
	}

	@SuppressWarnings("unchecked")
	@Override
	public List<IParam> split() {
		paramsList = new ArrayList<IParam>();
		dir = param.getValue(ParamKey.dir, null);
		if (dir == null) {
			LOGGER.error("Can't find the param [" + ParamKey.dir
					+ "] in sftp-spliter-param.");
			return paramsList;
		}

		String[] dirs = StringUtils.split(dir, ',');
		for (int i = 0; i < dirs.length; i++) {
			String oneDir = dirs[i];
			
			// if (oneDir.endsWith("*")) {
			// oneDir = oneDir.substring(0, oneDir.lastIndexOf('*'));
			// }

			if (oneDir.endsWith("/")) {
				oneDir = oneDir.substring(0, oneDir.lastIndexOf('/'));
			}

			uri = URI.create(oneDir);
			scheme = uri.getScheme();
			host = uri.getHost();
			port = uri.getPort();
			path = uri.getPath();
			username = uri.getUserInfo();
			password = param.getValue(ParamKey.password, this.password);

			if (!scheme.equalsIgnoreCase("sftp") || StringUtils.isBlank(host)
					|| UNDEFINED_PORT == port || StringUtils.isBlank(path)
					|| StringUtils.isBlank(username)
					|| StringUtils.isBlank(password)) {
				throw new IllegalArgumentException(
						"paramkey dir is not set properly, the correct sftp path format like: "
								+ "sftp://[<user>@]<host>[:<port>]/<path>/<file>");
			}

			PCInfo pi = new PCInfo();
			pi.setIp(host);
			pi.setPort(port);
			pi.setUser(username);
			pi.setPwd(password);
			pi.setPath(path);

			try {
				session = jsch.getSession(username, host, port);
				session.setUserInfo(pi);
				session.connect();

				channel = session.openChannel("sftp");
				channel.connect();
				c = (ChannelSftp) channel;

				if (c != null) {
					Boolean containsWildcardCharacter = false;
					Boolean isDirectory = false;
					try {
						SftpATTRS ss = c.lstat(path);
						isDirectory = ss.isDir();
					} catch (SftpException e) {
						containsWildcardCharacter = true;
					}
					
					Vector<LsEntry> files = (Vector<LsEntry>) c.ls(path);
					for (LsEntry lsEntry : files) {
						SftpATTRS sftpAttrs = lsEntry.getAttrs();
						
						if (sftpAttrs == null) {
							throw new IllegalArgumentException(
									"paramkey dir not found on the remote server: "
											+ lsEntry.getFilename());
						} else if (sftpAttrs.isDir()) {
							LOGGER.info(lsEntry.getFilename()
									+ " is a directory, permission string is "
									+ sftpAttrs.getPermissionsString());
						} else {
							LOGGER.info(lsEntry.getFilename()
									+ " is a file, permission string is "
									+ sftpAttrs.getPermissionsString());
						}
						
						if (lsEntry.getFilename().startsWith(".")
								|| lsEntry.getFilename()
										.startsWith("_")
								|| lsEntry.getFilename().endsWith(
										".index"))
							continue;

						if (lsEntry.getAttrs().isDir()) {
							continue;
						}

						IParam oParams = param.clone();
						
						String dir = oneDir;
						String absolutePath = null;
						if (containsWildcardCharacter){
							dir = oneDir.substring(0, oneDir.lastIndexOf('/'));
							absolutePath = dir + "/"
							+ lsEntry.getFilename();
						}else {
							if (!isDirectory){
								absolutePath = dir;
							}else{
								absolutePath = dir + "/" + lsEntry.getFilename();
							}
						}
						
						LOGGER.info(ParamKey.dir + " split filename:"
								+ absolutePath + "\tfile length:"
								+ lsEntry.getAttrs().getSize());

						oParams.putValue(ParamKey.dir, absolutePath);
						paramsList.add(oParams);
						
					}
				}
				c.disconnect();
				session.disconnect();

			} catch (Exception e) {
				c.disconnect();
				session.disconnect();
				LOGGER.error("something wrong with jsch:" + e.getCause());
			}
		}

		LOGGER.info("the number of splitted files: " + paramsList.size());
		return paramsList;
	}

	public static void main(String[] args) {
		SftpDirSplitter s = new SftpDirSplitter();
		HashMap<String, String> paramMap = new HashMap<String, String>();
		paramMap.put(ParamKey.dir,
				"sftp://hadoop@192.168.7.80:58422/home/hadoop/wormhole-sftp-test/test*");
		paramMap.put(ParamKey.password, "hadoopdev");
		IParam params = new DefaultParam(paramMap);
		s.init(params);
		s.split();
	}
}
