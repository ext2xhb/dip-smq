package com.hong.dip.smq.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.hong.dip.smq.transport.MessageCtrlException.FatalMessageException;
import com.hong.dip.smq.transport.MessageCtrlException.TemporaryMessageException;

/**
 * @author xuhb
 *
 */
public interface MessageWriter {
	String getMessageId();

	/**
	 * 检查当前Writer是否正在写入此消息
	 * @param msgId
	 * @return
	 */
	boolean isWritingMsg(String msgId);
	/**
	 * 开始写入新的消息
	 * @param msgId
	 * @param string2List
	 * @param partNum
	 * @throws IOException
	 */
	void startNewMessage(String msgId, List<String> string2List, int partNum) throws IOException;


	/**
	 * write a chunk data to message storage
	 * @param partIndex
	 * @param partLength
	 * @param chunkLen
	 * @param inputStream
	 * @throws TemporaryMessageException
	 * @throws FatalMessageException
	 */
	void writeChunk(int partIndex, long partLength, long chunkIndex, int chunkLen, InputStream inputStream) throws TemporaryMessageException, FatalMessageException;

	/**
	 * 检查消息内容的完整性，并返回需要继续写入的位置
	 * msgId 要检查的消息的id
	 * partNum	要检查的消息所拥有的part总数。
	 * @return MessagePosition partInde表示需要从该part继续写入消息内容，chunkIndex表示从part的chunkIndex处继续写入消息内容。chunkIndex=-1表示当前part的内容是完整的。是否需要继续写入由发送方判断
	 * 目前的实现方式下，chunkIndex=-1表示当前消息的内容完整，无需写入。
	 */
	MessagePosition checkPositionToWrite(String msgId, int partNum) throws IOException;
	
	void open() throws IOException;
	void close();
	


	public static class MessagePosition{
		private int partPos;
		private long chunkPos;
		public MessagePosition(int partIdx, long chunkIdx){
			this.partPos = partIdx;
			this.chunkPos = chunkIdx;
		}
		public int getPartIndex() {
			return partPos;
		}
		public long getChunkIndex() {
			return chunkPos;
		}
		
	}



	



}
