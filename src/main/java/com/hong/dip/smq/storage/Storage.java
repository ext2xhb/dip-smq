package com.hong.dip.smq.storage;

import org.apache.camel.Service;

public interface Storage extends Service{
	public QueueStorage getOrCreateQueueStorage(String qname) throws Exception;

	public void deleteQueueStorage(String name);
	
}
