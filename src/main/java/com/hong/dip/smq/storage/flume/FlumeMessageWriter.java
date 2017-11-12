package com.hong.dip.smq.storage.flume;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.storage.MessageWriter;
import com.hong.dip.smq.transport.MessageCtrlException.FatalMessageException;
import com.hong.dip.smq.transport.MessageCtrlException.TemporaryMessageException;

public class FlumeMessageWriter implements MessageWriter {
	static final Logger log = LoggerFactory.getLogger(FlumeMessageWriter.class);
	FlumeQueueStorage queueStorage;
	private String remoteQueue;
	private String msgId;
	private List<String> attachmentNames;
	private int partNum;
	private File attachmentDir; //保存附件的目录
	private AttachmentWriter attachmentWriter = new AttachmentWriter();
	
	public FlumeMessageWriter(FlumeQueueStorage queueStorage, File attachmentDir, String remoteQueue) {
		this.queueStorage = queueStorage;
		this.remoteQueue = remoteQueue;
		this.attachmentDir = attachmentDir;
	}

	@Override
	public String getMessageId() {		// TODO Auto-generated method stub
		return msgId;
	}
	/* 
	 * 由于我们最后保存消息体到Flume队列，所以写入msgID等metas时只需要在内存中记录就可以，无需序列化到磁盘
	 */
	@Override
	public void writeMeta(String msgId, List<String> attachmentNames, int partNum) {
		this.msgId = msgId;
		this.attachmentNames = attachmentNames;
		this.partNum = partNum;
		
	}

	
	@Override
	public void writeChunk(int partIndex, long partLength, int chunkLen, InputStream inputStream) 
			throws TemporaryMessageException, FatalMessageException {
		if(partIndex < partNum - 1){ //if attachments;
			writeAttachment(this.attachmentNames.get(partIndex), partIndex, chunkLen, inputStream);
		}else{//write body
			writeBody(chunkLen, inputStream, partIndex, partNum); //write body to flumen queue and save message check point
		}
	}

	private void writeBody(int chunkLen, InputStream inputStream, int partIndex, int partNum) throws TemporaryMessageException {
		byte[] b;
		try {
			b = readAll(chunkLen, inputStream);
		} catch (IOException e) {
			TemporaryMessageException e1 = new TemporaryMessageException(
				"Temporary error occurs. cannot read message from http server for queue("+this.queueStorage.getName()+")", e);
			log.error(e1.getMessage(), e1.getCause());
			throw e1;
		}
		
		FlumeMessageStorage msg= new FlumeMessageStorage();
		msg.setID(this.msgId);
		msg.setAttachmentNames(this.attachmentNames);
		msg.getEvent().setBody(b);
		
		
		try {
			//save message to queue
			if(!this.queueStorage.offer(msg)){
				TemporaryMessageException e1 = new TemporaryMessageException("Temporary error occurs. queue("+queueStorage.getName()+") is full", new Exception("queu full"));
				log.error(e1.getMessage(), e1.getCause());
				throw e1;
			}
			//write check log for  this message;
			FlumeMessageCheckLog checkLog = queueStorage.getFlumeMessageCheckLog();
			checkLog.saveLog(remoteQueue, msg.getID(), partIndex, partNum);
			//commit this message, if failed, restore check log to previous message;
			try{
				this.queueStorage.commit();
			}catch(Exception e){
				checkLog.restoreLog(remoteQueue);
				throw e;
			}
			//TODO 目前的实现，在非常罕见的情况下，消息已经提交到队列，但是当前消息的checkLog没有写入，那么会导致消息内容重复。

		} catch (Exception e) {
			try {
				queueStorage.rollback();
			} catch (Exception e2) {
				log.error("Rollback as queue offer error or save log failure", e);
			}
			TemporaryMessageException e1 = new TemporaryMessageException(
					"Temporary error occurs. Cannot put msg to flumequeue("+this.queueStorage.getName()+")", e);
			log.error(e1.getMessage(), e1.getCause());
			throw e1;
		}
		
	}

	/**
	 */
	private byte[] readAll(int chunkLen, InputStream inputStream) throws IOException {
		byte[] b = new byte[chunkLen]; 
		
		int off = 0;
		int len = b.length - off;
		int readed = 0;
		while((readed = inputStream.read(b, off, len)) >= 0){
			off += readed;
			len -= readed;
		}
		return b;
	}


	/**
	 * @param attachmentName	 attachment names
	 * @param partIndex			attachment's index;
	 * @param chunkLen			chunkLen;
	 * @param inputStream		chunk content
	 * @throws IOException
	 */
	private void writeAttachment(String attachmentName, int partIndex, int chunkLen, InputStream inputStream) throws TemporaryMessageException{
		try{
			this.attachmentWriter.prepareForWrite(this.msgId, attachmentName, partIndex);
			this.attachmentWriter.writeChunk(chunkLen, inputStream);
		}catch(IOException e){
			TemporaryMessageException e1 = new TemporaryMessageException(
				"Cannot write attachment ("+attachmentName+") of message("+this.msgId+")", e);
			log.error(e1.getMessage(), e);
			throw e1;
		}
	}


	@Override
	public MessagePosition checkAndSetWritePosition() throws IOException {
		return attachmentWriter.checkAndSetWritePosition();
	}

	@Override
	public void open() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
	
	class AttachmentWriter {
		File currFile = null;
		FileOutputStream fos = null;
		int currentPartIndex = -1; //正在写入的attachment的index
		public void prepareForWrite(String msgId, String attachmentName, int partIndex) throws IOException{
			if(partIndex != currentPartIndex){
				closeCurrentPart();
				openPart(partIndex);
				this.currentPartIndex = partIndex;
				
			}
		}
		
		private void openPart(int partIndex) throws IOException {
			File attachment = this.getAttachmentFile(partIndex);
			fos = new FileOutputStream(attachment);
			currFile = attachment;
		}
		private void closeCurrentPart() throws IOException{
			try{
				if(fos != null){
					fos.close();
					fos = null;
					currFile = null;
				}
			}catch(IOException e){
				log.error("Error Occurs. Cannot close attachment file("+currFile+")", e);
			}
		}

		public void writeChunk(int chunkLen, InputStream inputStream) throws IOException{
			byte[] b = new byte[1024];
			int left = chunkLen;
			int readed;
			while((readed = inputStream.read(b)) != -1){
				fos.write(b, 0, readed);
				left -= readed;
			}
			if(left !=  0){
				throw new IOException("Error Occurs. Chunk InpuStream's length is not equals with chunkLen; remoteQueue("+remoteQueue+") message ("+msgId+")");
			}
			
		}

		/**
		 * 检查所有的part（包含attachment和body),返回确认无疑已经保存的part index , chunk index
		 * @return
		 */
		public MessagePosition checkAndSetWritePosition() throws IOException {
			//关闭当前正在打开处理的Part因为我们检查时使用的是磁盘上的文件元数据信息，它和内存中的文件对象信息不一定一致。
			closeCurrentPart();  
			long resultChunkIndex = checkChunkIndexOfPart(0);  
			int resultPartIndex = 0;
			for(int partIndex = 1; partIndex < partNum; partIndex++){
				boolean partExist = false;
				long chunkIndex = checkChunkIndexOfPart(partIndex);
				if(chunkIndex == -2){ //no this part, prev part's check result is ok
					break; //prev is ok
				}
				else { //chunk index may be -1(complete) or >=0
					resultChunkIndex = chunkIndex;
					resultPartIndex = partIndex;
				}
			}
			
			return new MessagePosition(resultPartIndex, resultChunkIndex);
		}
		// check the uncomplete chunk to write;
		// return >= 0: index of chunk which is uncomplete, -1: part is full; -2: not part content
		private long checkChunkIndexOfPart(int partIndex) {
			if(partIndex < partNum - 1){
				return checkChunkIndexOfAttachmentPart(partIndex);
			}else
				return checkChunkIndexOfBodyPart();
		}

		private long checkChunkIndexOfBodyPart() {
			if(queueStorage.getMsgCheckLog().checkIfBodySaved(remoteQueue, msgId)){
				return -1; // body saved in flume queue storage;
			}else{
				return -2; //body part not saved;
			}
		}

		private long checkChunkIndexOfAttachmentPart(int partIndex) {
			File attachment = this.getAttachmentFile(partIndex);
			if(attachment.exists()){
				long flen = attachment.length();
				long chunkIndex = flen / queueStorage.getChunkSize();
				//文件长度是chunk的整数倍的时候，丢弃最后一个chunk，以确保正确性
				if(flen % queueStorage.getChunkSize() == 0)
					chunkIndex--;
				if(chunkIndex < 0) chunkIndex = 0;
				return chunkIndex;
				
			}else
				return -2;
		}

		/**
		 * @param partIndex : partIndex of attachment; must not be body part's index
		 * @return part
		 */
		private File getAttachmentFile(int partIndex) {
			String fname = new StringBuilder(remoteQueue).append('-')
					.append(msgId).append('-')
					.append(partIndex).append('-')
					.append(attachmentNames.get(partIndex)).toString();
			return new File(attachmentDir, fname);
		}
		
	}
}
