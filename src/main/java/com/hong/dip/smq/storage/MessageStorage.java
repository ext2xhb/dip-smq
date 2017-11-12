package com.hong.dip.smq.storage;

import java.util.List;

import com.hong.dip.smq.ChunkableDataSource;

public interface MessageStorage {

	String getID();
	void setID(String msgId);
	
	public long getStoreSequence();
	public void setStoreSequence(long sequence);
	/**
	 * 获取消息体内容。必须是QueueStorage支持的内容和格式
	 * @param c
	 * @return
	 */
	<T> T getBodyContent(Class<T> c);
	
	/**
	 * 加载存储中的附件内容
	 */
	void loadAttachments() throws Exception;
	
	void addPart(ChunkableDataSource ds);
	List<ChunkableDataSource> getParts();
}
