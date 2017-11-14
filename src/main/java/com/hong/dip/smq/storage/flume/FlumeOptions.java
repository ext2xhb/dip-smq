package com.hong.dip.smq.storage.flume;

import java.io.File;

public class FlumeOptions {
	
	private String storagePath;
	private int putWaitSeconds = 3; //time wait for queue space available 
	private int defaultQueueDepth = 100000;
	private int chunkSize = 1024*1024; // 大消息的分片大小
	private int transactionCapacity = 10; //transaction内可以保存的最大消息数（未提交）
	public FlumeOptions(String storagePath){
		this.storagePath = storagePath;
	}
	
	public String __getStoragePath() {
		return storagePath;
	}

	public void setPutWaitSeconds(int waitSeconds) {
		this.putWaitSeconds = waitSeconds;
	}

	public int getPutWaitSeconds() {
		return putWaitSeconds;
	}

	public int getDefaultQueueDepth() {
		return defaultQueueDepth ;
	}

	public void setDefaultQueueDepth(int queueDepth) {
		this.defaultQueueDepth = queueDepth;
	}

	public int getChunkSize() {
		return chunkSize;
	}

	public void setChunkSize(int chunkSize) {
		this.chunkSize = chunkSize;
	}


	public int getTransactionCapacity() {
		return transactionCapacity  > defaultQueueDepth ? defaultQueueDepth : transactionCapacity;
	}

	public void setTransactionCapacity(int transactionCapacity) {
		this.transactionCapacity = transactionCapacity;
	}
	
	
}
