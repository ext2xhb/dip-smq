package com.hong.dip.smq;

import com.hong.dip.smq.storage.QueueStorage;

public interface Queue {
	public QueueStorage getStorage();
	/**
	 * @param m message to put 
	 * @return true: success; false: failed as full.
	 * @throws Exception(un-excepted exception);
	 */
	public boolean putMessage(Message m) throws Exception;
	/**
	 * @return message taked from queue , null : empty 
	 * @throws Exception unexcepted exception
	 */
	public Message getMessage(int millis) throws Exception;
	
	public void commit() throws Exception;
	public void rollback() throws Exception;
	
	public String getName();
	
	/**
	 * create a empty message with id
	 * @return
	 */
	public Message createMessage();

}
