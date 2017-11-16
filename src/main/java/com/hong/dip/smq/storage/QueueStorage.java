package com.hong.dip.smq.storage;

import java.io.IOException;

import org.apache.camel.Service;

import com.hong.dip.smq.Message;
import com.hong.dip.smq.MessagePostHandler;
import com.hong.dip.smq.Queue;

/**
 * @author xuhb
 * 队列存储接口
 * 1：取消息
 * 2：存消息
 * 3：
 */
public interface QueueStorage extends Service{
	public String getName();
	
	public Queue getQueue(); //TODO 记得以后删除这个方法，使用另外的方式获得Queue

	public void commit() throws Exception;
	public void rollback() throws Exception;
	public boolean offer(MessageStorage msgStorage) throws Exception;
	public MessageStorage take(int milis) throws Exception;
	
	public int getChunkSize(); //分片大小：限制了一次读取的大小.
	
	public MessageStorage messageToStorage(Message message);
	public Message storageToMessage(MessageStorage messageStorage);
	
	public String newMessageID();
	public long newMessageSequence();
	//public ChunkableDataSource getAttachmentStore(MessageStorage message, int idx, String name)/* throws Exception*/;

	/**
	 * 如果有则返回当前消息对应的writer，如果没有则尝试从磁盘打开对应消息的Writer；
	 * @param remoteQueue
	 * @param msgId
	 * @return
	 * @throws IOException
	 */
	public MessageWriter getOrOpenMessageWriter(String remoteQueue, String msgId) throws IOException;

	public MessageWriter getCurrentMessageWriter(String remoteQueue);

	public void setMessagePostHandler(MessagePostHandler handler);
	public MessagePostHandler getMessagePostHandler();
	

}
