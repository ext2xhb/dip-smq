package com.hong.dip.smq;

import java.io.IOException;

import com.hong.dip.smq.storage.QueueStorage;

public class RemoteQueue implements Queue {
	
	private QueueStorage storage;
	private String destQueue;
	private Node destNode;
	public RemoteQueue(Node destNode, String destQueue, QueueStorage storage){
		this.storage = storage;
		this.destNode = destNode;
		this.destQueue = destQueue;
	}
	public String getDestQueueName(){
		return destQueue;
	}
	public Node getDestNode(){
		return this.destNode;
	}
	@Override
	public QueueStorage getStorage() {
		return storage;
	}

	@Override
	public boolean putMessage(Message m) throws Exception {
		return storage.getQueue().putMessage(m);
	}

	@Override
	public Message getMessage(int millis) throws Exception {
		throw new IOException("Cannot get message from Remote-Queue");
	}

	@Override
	public void commit() throws Exception {
		storage.getQueue().commit();
		
	}

	@Override
	public void rollback() throws Exception {
		storage.getQueue().rollback();
		
	}

	@Override
	public String getName() {
		return storage.getQueue().getName();
	}

	@Override
	public Message createMessage() {
		return storage.getQueue().createMessage();
	}

}
