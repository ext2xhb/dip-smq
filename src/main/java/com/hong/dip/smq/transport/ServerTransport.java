package com.hong.dip.smq.transport;

import com.hong.dip.smq.Queue;

public interface ServerTransport {
	public void start() throws Exception;
	public void stop() throws Exception;
	public void registerMessageReceiver(Queue queue) throws Exception;
}
