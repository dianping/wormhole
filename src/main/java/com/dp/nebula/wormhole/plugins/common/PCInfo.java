package com.dp.nebula.wormhole.plugins.common;

import com.jcraft.jsch.UserInfo;

public class PCInfo implements UserInfo {
	private String ip;
	private int port;
	private String user;
	private String pwd;
	private String path;

	public String getIp() {
		return ip;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getPwd() {
		return pwd;
	}

	public void setPwd(String pwd) {
		this.pwd = pwd;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@Override
	public String getPassphrase() {
		return null;
	}

	@Override
	public String getPassword() {
		return getPwd();
	}

	@Override
	public boolean promptPassphrase(String message) {
		return true;
	}

	@Override
	public boolean promptPassword(String message) {
		return true;
	}

	@Override
	public boolean promptYesNo(String message) {
		return true;
	}

	@Override
	public void showMessage(String message) {
	}

	@Override
	public String toString() {
		return "PCInfo [ip=" + ip + ", path=" + path + ", port=" + port
				+ ", pwd=" + pwd + ", user=" + user + "]";
	}
}
