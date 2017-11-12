package com.hong.dip.smq.storage;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import com.hong.dip.smq.transport.MessageCtrlException.FatalMessageException;
import com.hong.dip.smq.transport.MessageCtrlException.TemporaryMessageException;

public interface MessageWriter {
	String getMessageId();

	void writeMeta(String msgId, List<String> string2List, int partNum);


	/**
	 * write a chunk data to message storage
	 * @param partIndex
	 * @param partLength
	 * @param chunkLen
	 * @param inputStream
	 * @throws TemporaryMessageException
	 * @throws FatalMessageException
	 */
	void writeChunk(int partIndex, long partLength, int chunkLen, InputStream inputStream) throws TemporaryMessageException, FatalMessageException;

	/**
	 * 检查消息内容的完整性，返回的消息位置表示，从该位置补全后内容才完整。
	 * 当partPos超过消息的最大的partIndex时，消息已经完整。
	 * @return
	 */
	MessagePosition checkAndSetWritePosition() throws IOException;
	
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
