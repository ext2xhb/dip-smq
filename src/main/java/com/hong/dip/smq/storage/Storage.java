package com.hong.dip.smq.storage;

public interface Storage {
	public QueueStorage getOrCreateQueueStorage(String qname) throws Exception;
	public void stop() throws Exception;
}
