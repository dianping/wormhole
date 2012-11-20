package com.dp.nebula.wormhole.plugins.writer.sftpwriter;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;
import com.dp.nebula.wormhole.plugins.common.PCInfo;
import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpATTRS;

public class SftpWriterPeriphery implements IWriterPeriphery {
	private static final Logger logger = Logger.getLogger(SftpWriterPeriphery.class);
	
	private String dir = "";
	private String prefixname = "part";
	
	private String scheme = "";
	private String host = "";
	private int port = 58422;
	private String path = "";
	private String username = "";
	private String password = "";
	private URI uri = null;
	
	private JSch jsch = null;
	private Session session = null;
	private Channel channel = null;
	private ChannelSftp c = null;
	
	@Override
	public void rollback(IParam param) {
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter) {
		logger.info("doPost stage do nothing");
	}

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		dir = param.getValue(ParamKey.dir, this.dir);
		prefixname = param.getValue(ParamKey.prefixname, this.prefixname);
		password = param.getValue(ParamKey.password, this.password);
		if (dir.endsWith("*")) {
			dir = dir.substring(0, dir.lastIndexOf('*'));
		}
		if (dir.endsWith("/")) {
			dir = dir.substring(0, dir.lastIndexOf('/'));
		}
		
		uri = URI.create(dir);
		scheme = uri.getScheme();
		host = uri.getHost();
		port = uri.getPort();
		path = uri.getPath();
		username = uri.getUserInfo();
		
		if (!scheme.equalsIgnoreCase("sftp") || StringUtils.isBlank(host) ||
				-1 == port || StringUtils.isBlank(path) ||
				StringUtils.isBlank(username) || StringUtils.isBlank(password)){
			throw new IllegalArgumentException(
					"paramkey dir is not set properly, the correct sftp path format like: " +
					"sftp://[<user>@]<host>[:<port>]/<path>/<file>");
		}
		
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
			
			SftpATTRS sftpAttrs = c.lstat(path);
			if (sftpAttrs == null){
				throw new IllegalArgumentException(
						"paramkey dir not found on the remote server: " + path);
			}else if (sftpAttrs.isDir() ){
				logger.info("removing files under the " + path);
				c.rm(path + "/" + prefixname + "*");
			}else{
				logger.error(path + " is a file, please make sure it is only a directory. ");
				return;
			}
		} catch (Exception e) {
			closeAll();
			throw new RuntimeException("something wrong with jsch:" 
					+ e.getCause());
		}
		closeAll();
	}
	
	private void closeAll(){
		if (c != null) {
			c.disconnect();
		}
		if (session != null) {
			session.disconnect();
		}
	}
}
