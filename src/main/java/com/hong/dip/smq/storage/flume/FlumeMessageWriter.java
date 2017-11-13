package com.hong.dip.smq.storage.flume;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.storage.MessageWriter;
import com.hong.dip.smq.transport.MessageCtrlException.FatalMessageException;
import com.hong.dip.smq.transport.MessageCtrlException.TemporaryMessageException;
import com.hong.dip.utils.StringUtils;

public class FlumeMessageWriter implements MessageWriter {
	static final Logger log = LoggerFactory.getLogger(FlumeMessageWriter.class);
	FlumeQueueStorage queueStorage;
	private String remoteQueue;
	private File attachmentDir; //保存附件的目录

	
	private String msgId;
	private List<String> attachmentNames;
	private int partNum;
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
	@Override
	public boolean isWritingMsg(String msgId){
		return StringUtils.strEquals(msgId, this.msgId);
	}
	/* 
	 */
	@Override
	public void startNewMessage(String msgId, List<String> attachmentNames, int partNum) throws IOException {
		this.msgId = msgId;
		this.attachmentNames = attachmentNames;
		this.partNum = partNum;
		this.attachmentWriter.closeCurrentPart();
	}

	
	@Override
	public void writeChunk(int partIndex, long partLength, long chunkOffset, int chunkLen, InputStream inputStream) 
			throws TemporaryMessageException, FatalMessageException {
		if(partIndex < partNum - 1){ //if attachments;
			writeAttachment(this.attachmentNames.get(partIndex), partIndex, partLength, chunkOffset, chunkLen, inputStream);
		}else{//write body
			writeBody(chunkLen, inputStream, partIndex, partNum); //write body to flumen queue and save message check point
		}
	}

	private void writeBody(int chunkLen, InputStream inputStream, int partIndex, int partNum) throws TemporaryMessageException {
		byte[] b;
		try {
			//在写入body之前，关闭Attachment
			this.attachmentWriter.closeCurrentPart();
			
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
		if(partIndex > 0){
			List<String> attachmentPaths = new ArrayList<String>(partIndex);
			for(int i = 0; i < partIndex; i++){
				attachmentPaths.add(attachmentWriter.getAttachmentFile(i).getPath());
			}
			msg.setAttachmentPaths(attachmentPaths);
		}
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
				checkLog.restoreLog(remoteQueue); //当队列提交失败的时候回复check日志，
				throw e;
			}

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
		if(len != 0)
			throw new IOException("Cannot read a complete chunk from inputstream; queue("+this.remoteQueue+") msgId("+this.msgId+")");
		return b;
	}


	/**
	 * @param attachmentName	 attachment names
	 * @param partIndex			attachment's index;
	 * @param chunkOffset		chunk在文件中的offset
	 * @param chunkLen			chunkLen;
	 * @param inputStream		chunk content
	 * @throws IOException
	 */
	private void writeAttachment(String attachmentName, int partIndex, long partLength, long chunkOffset, int chunkLen, InputStream inputStream) throws TemporaryMessageException{
		try{
			this.attachmentWriter.prepareForWrite(attachmentName, partIndex, partLength, chunkOffset);
			this.attachmentWriter.writeChunk(chunkLen, inputStream);
		}catch(IOException e){
			TemporaryMessageException e1 = new TemporaryMessageException(
				"Cannot write attachment ("+attachmentName+") of message("+this.msgId+")", e);
			log.error(e1.getMessage(), e);
			throw e1;
		}
	}


	@Override
	public MessagePosition checkPositionToWrite(String msgId,int partNum) throws IOException {
		//发送方准备发送的消息与当前正在写入的消息不一样，那么根据check-log检查该消息是否已经写入成功了
		//如果写入成功返回消息已经完整的Position。如果不成功，那么强制从消息的开始位置发送
		//TODO	注意：由于当前check-log只记录最后一条成功的消息。如果要检查的消息不是check-log记录的最后一条消息，我们总是假设该消息是新的消息需要重新发送
		
		if(this.msgId == null || !this.msgId.equals(msgId)) { //
			FlumeMessageCheckLog checkLog = this.queueStorage.getFlumeMessageCheckLog();
			if(checkLog.checkIfBodySaved(remoteQueue, msgId)){
				return new MessagePosition(partNum-1, -1); // ending positon of message's last part;
			}else
				return new MessagePosition(0, 0);
		}
		else
			return attachmentWriter.checkAndSetWritePosition();
	}

	@Override
	public void open() throws IOException {
	}

	@Override
	public void close() {
	}
	
	class AttachmentWriter {
		File currFile = null;
		RandomAccessFile fos = null;
		int currentPartIndex = -1; //正在写入的attachment的index
		long currentPartLength = 0; //当前part还剩余多少字节没有写入
		long writedPartLength = 0;
		public void prepareForWrite(String attachmentName, int partIndex, long partLength, long chunkOffset) throws IOException{
			if(partIndex != currentPartIndex){
				closeCurrentPart();
				openPart(partIndex, partLength, chunkOffset);
			}
		}
		
		private void openPart(int partIndex, long partLength, long chunkOffset) throws IOException {
			File attachment = this.getAttachmentFile(partIndex);
			StringUtils.ensureFileExists(attachment);
			fos = new RandomAccessFile(attachment, "rw");
			fos.seek(chunkOffset);
			
			currFile = attachment;
			currentPartLength = partLength;
			writedPartLength = chunkOffset;
			
			currentPartIndex = partIndex;
		}
		private void closeCurrentPart() throws IOException{
			try{
				if(fos != null){
					fos.close();
					fos = null;
					currFile = null;
					currentPartIndex = -1;
					currentPartLength =  0;
					writedPartLength = 0;
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
				throw new IOException("Error Occurs. Chunk InpuStream's length is not equals with chunkLen; remoteQueue("+remoteQueue+") message ("+msgId+") chunk("+this.currFile+")");
			}
			writedPartLength += chunkLen;
			if(writedPartLength >= this.currentPartLength){
				this.closeCurrentPart();
				if(writedPartLength > currentPartLength){
					throw new IOException("Error Occurs. Attachment("
						+ this.currFile+") received too many bytes("
						+ writedPartLength+") than part-length("
						+ currentPartLength+")");
				}
			}
			
		}

		/**
		 * 根据磁盘存储的信息，重新核对消息的所有的part（包含attachment和body)，返回需要继续写入的Part的位置
		 * @return	MessagePosition：需要写入内容的消息位置，如果当前消息完整MessagePosition的partIndex指向最后一个Part，chunkIndex　为-1;
		 */
		public MessagePosition checkAndSetWritePosition() throws IOException {
			//关闭当前正在打开处理的Part。准备根据磁盘存储信息核对消息内容
			closeCurrentPart();  
			long resultChunkIndex = checkChunkIndexOfPart(0);  
			int resultPartIndex = 0;
			for(int partIndex = 1; partIndex < partNum; partIndex++){
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
