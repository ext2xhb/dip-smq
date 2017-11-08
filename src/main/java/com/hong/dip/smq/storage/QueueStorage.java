package com.hong.dip.smq.storage;

import org.apache.camel.Service;

import com.hong.dip.smq.ChunkableDataSource;
import com.hong.dip.smq.Message;
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

	public String allocateMessageID();


	public ChunkableDataSource getAttachmentStore(MessageStorage message, int idx, String name)/* throws Exception*/;

}
