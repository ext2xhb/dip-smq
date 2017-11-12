package com.hong.dip.smq.transport;

import org.apache.camel.Service;

import com.hong.dip.smq.storage.QueueStorage;

public interface ServerTransport extends Service{
	/**
	 * 注册Queue，注册后Transport会接收Queue对应的消息。
	 * @param queue
	 * @throws Exception
	 */
	public void startMessageReceiver(QueueStorage queue) throws Exception;

	public void stopMessageReceiver(QueueStorage queue);
}
