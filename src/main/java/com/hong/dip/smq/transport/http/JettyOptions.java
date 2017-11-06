package com.hong.dip.smq.transport.http;

public class JettyOptions {
	private int minThreads = 0;
	private int maxThreads = 0;
	private String threadPoolName = "JETTY_THREAD_POOL";
	private String host = "0.0.0.0";
	private int port = 8080;
	private boolean isReuseAddress = false;
	private long maxIdleTime = 60000; //mili-seconds for max idle time, 0 is invalide as it means never-time-out
	
	public boolean isReuseAddress() {
		return isReuseAddress;
	}
	public void setReuseAddress(boolean isReuseAddress) {
		this.isReuseAddress = isReuseAddress;
	}
	public int getMinThreads() {
		return minThreads;
	}
	public String getHost() {
		return host;
	}
	public void setHost(String host) {
		this.host = host;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public void setMinThreads(int minThreads) {
		this.minThreads = minThreads;
	}
	public int getMaxThreads() {
		return maxThreads;
	}
	public void setMaxThreads(int maxThreads) {
		this.maxThreads = maxThreads;
	}
	public String getThreadPoolName() {
		return threadPoolName;
	}
	public void setThreadPoolName(String threadPoolName) {
		this.threadPoolName = threadPoolName;
	}
	
	/*
	 * 保留属性
	 */
	public boolean getSendServerVersion() {
	
		return false;
	}
	
	public long getMaxIdleTime() {
		return maxIdleTime;
	}
	public void setMaxIdleTime(long maxIdleTime){
		this.maxIdleTime = maxIdleTime;
	}
	
}
