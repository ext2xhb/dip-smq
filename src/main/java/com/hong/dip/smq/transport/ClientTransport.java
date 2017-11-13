package com.hong.dip.smq.transport;

import org.apache.camel.Service;

import com.hong.dip.smq.Node;
import com.hong.dip.smq.RemoteQueue;
import com.hong.dip.smq.storage.QueueStorage;

public interface ClientTransport extends Service{

	void startMessageSender(RemoteQueue remoteQueue, QueueStorage queue);

	void stopMessageSender(QueueStorage queue);

}
