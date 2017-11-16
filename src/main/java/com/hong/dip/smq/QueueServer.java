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
	public RemoteQueue createRemoteQueue(String qname, Node node, MessagePostHandler postHandler) throws Exception;
	
	/**
	 * 删除Queue,暂时不支持。
	 * @param queue
	 * @throws Exception
	 */
	public void deleteQueue(Queue queue) throws Exception;
	
}
