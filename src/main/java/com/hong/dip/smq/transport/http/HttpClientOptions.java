package com.hong.dip.smq.transport.http;

public class HttpClientOptions {
	private long connLivingTime = Long.MAX_VALUE; //socket connection's living time in connection pool; milliseconds
	private int connCheckInterval = 2000; //connection status checking interval when it's idle; milliseconds
	private int soTimeOut = 3000; 
	private boolean soKeepAlive = true;
	private int connectionTimeout = 5000; //socket connection 连接超时
	private int socketTimeout = 5000;				//socket timeout
	private int connectionRequestTimeout = 5000; //http request timeout
	private int maxConnection = 200; //连接池上限（由于我们是长连接，所以这个数值并不关键）
	private int maxConnPerClient = 50; //HttpClient的实现需要约束每个Client的连接数上限，需要注意这个数值应该大于我们的Client节点上的队列数。
	
	public int getMaxConnection() {
		return maxConnection;
	}
	public void setMaxConnection(int maxConnection) {
		this.maxConnection = maxConnection;
	}

	public int getMaxConnPerClient() {
		return maxConnPerClient;
	}
	public void setMaxConnPerClient(int maxConnPerClient) {
		this.maxConnPerClient = maxConnPerClient;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public void setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
	}

	public int getConnectionRequestTimeout() {
		return connectionRequestTimeout;
	}

	public void setConnectionRequestTimeout(int connectionRequestTimeout) {
		this.connectionRequestTimeout = connectionRequestTimeout;
	}

	public boolean isSoKeepAlive() {
		return soKeepAlive;
	}

	public void setSoKeepAlive(boolean soKeepAlive) {
		this.soKeepAlive = soKeepAlive;
	}

	public void setSoTimeOut(int soTimeOut) {
		this.soTimeOut = soTimeOut;
	}
	public int getSoTimeOut() {
		return soTimeOut;
	}
	public int getConnCheckInterval() {
		return connCheckInterval;
	}

	public void setConnCheckInterval(int connCheckInterval) {
		this.connCheckInterval = connCheckInterval;
	}

	public long getConnLivingTime() {
		return connLivingTime;
	}

	public void setConnLivingTime(long connLivingTime) {
		this.connLivingTime = connLivingTime;
	}

	
}
