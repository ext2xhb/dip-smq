package com.hong.dip.smq;

public interface QueueServer {
	public void start() throws Exception;
	public void stop() throws Exception;
	/**
	 * create a local Queueu; 
	 * @param qname
	 * @return
	 * @throws Exception
	 */
	public Queue createQueue(String qname) throws Exception;
	public Queue createRemoteQueue(String qname, Node node) throws Exception;
	
}
