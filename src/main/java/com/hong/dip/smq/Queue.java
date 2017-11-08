package com.hong.dip.smq;

public interface Queue {
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
	public Message getMessage() throws Exception;
	
	public void commit() throws Exception;
	public void rollback() throws Exception;
	
	public String getName();
	
	/**
	 * create a empty message ( id is set)
	 * @return
	 */
	public Message createMessage();

}
