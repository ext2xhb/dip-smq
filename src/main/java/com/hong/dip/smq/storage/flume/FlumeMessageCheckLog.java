package com.hong.dip.smq.storage.flume;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.utils.StringUtils;

public class FlumeMessageCheckLog {
	static final Logger log = LoggerFactory.getLogger(FlumeMessageCheckLog.class);
	
	static final int STATUS_FINISH = 1;
	static final int STATUS_PARTIAL = 0;
	static final int QNAMESIZE = 100;
	
	static final int MSGIDSIZE = 50;
	static final int INTSIZE = 4;
	static final int MSGDESCSIZE = MSGIDSIZE + INTSIZE * 2; 
	static final int CHECKPOINTSIZE = QNAMESIZE + MSGDESCSIZE;  
	
	File checkLogFile;
	RandomAccessFile fileAccess;
	//key = queue name,    inside-key = msgId
	Map<String, CheckPoint> checkpoints = new ConcurrentHashMap<String, CheckPoint>();
	
	static class CheckPoint{
		String remoteQueue;
		long offsetInLog; // 保存在logfile中的偏移量
		MsgDesc[] msgs = new MsgDesc[2];
		
		static class MsgDesc{ //describe a message position
			String msgId = "";
			int partIndex;
			int partNum;
			MsgDesc(){
				
			}
			MsgDesc(String msgId, int partIndex, int partNum){
				this.msgId = msgId;
				this.partIndex = partIndex;
				this.partNum = partNum;
			}
		}

		CheckPoint(String remoteQueue){
			this.remoteQueue = remoteQueue;
			this.offsetInLog = -1;
			msgs = new MsgDesc[2];
			msgs[0] = new MsgDesc();
			msgs[1] = new MsgDesc();
		}
	}
	
	public FlumeMessageCheckLog(File checkLogFile) {
		this.checkLogFile = checkLogFile;
	}

	public void open() throws IOException{
		if(!checkLogFile.exists())
			StringUtils.mkEmptyFile(checkLogFile);
		fileAccess = new RandomAccessFile(checkLogFile, "rws");
		loadCheckPoint();
		
	}

	private void loadCheckPoint() throws IOException{
		byte[] b = new byte[CHECKPOINTSIZE];
		for(int i = 0; i < fileAccess.length() / CHECKPOINTSIZE; i++){
			if(fileAccess.read(b) != b.length)
				throw new IOException("CheckLog (" + this.checkLogFile +")corrupted, checkpoint log is notconsistence");
			CheckPoint chk = bytes2Checkpoint(b);
			chk.offsetInLog = i * CHECKPOINTSIZE;
			this.checkpoints.put(chk.remoteQueue, chk);
		}
	}
	private byte[] checkPoint2Bytes(CheckPoint chk){
		byte[] b = new byte[CHECKPOINTSIZE];
		StringUtils.writeString( chk.remoteQueue, b, 0, QNAMESIZE);
		
		int offset = 0;
		for(int i = 0; i < chk.msgs.length; i++){
			StringUtils.writeString(chk.msgs[i].msgId, b, offset + QNAMESIZE, MSGIDSIZE);
			StringUtils.writeInt(chk.msgs[i].partIndex, b, offset + QNAMESIZE + MSGIDSIZE);
			StringUtils.writeInt(chk.msgs[i].partNum, b, offset + QNAMESIZE + MSGIDSIZE + 4);
			offset += MSGDESCSIZE;
		}
		return b;
	}
	private CheckPoint bytes2Checkpoint(byte[] b) {
		String qname = StringUtils.readString(b, 0, QNAMESIZE);
		CheckPoint chk = new CheckPoint(qname);
		int offset = 0;
		for(int i = 0; i < chk.msgs.length; i++){
			String msgId = StringUtils.readString(b, offset + QNAMESIZE, MSGIDSIZE);
			int partIndex = StringUtils.readInt(b, offset + QNAMESIZE + MSGIDSIZE);
			int partNum = StringUtils.readInt(b, offset + QNAMESIZE + MSGIDSIZE + 4);
			chk.msgs[i] = new CheckPoint.MsgDesc(msgId, partIndex, partNum);
			offset += MSGDESCSIZE;
		}
		return chk;
	}

	public void close() {
		try {
			fileAccess.close();
		} catch (IOException e) {
			log.error("Failed to close Message check log file(" + this.checkLogFile + ")", e);
		}
	}

	/**
	 * 保存消息日志：目前只记录消息成功保存的part的index, part num, 
	 * @param msg
	 * @param finishedPartIndex
	 */
	public void saveLog(String remoteQueue, String msgId, int finishedPartIndex, int partNum) throws IOException{
		this.checkpoints.putIfAbsent(remoteQueue, new CheckPoint(remoteQueue));
		CheckPoint chk = checkpoints.get(remoteQueue);
		chk.msgs[1] = chk.msgs[0];
		chk.msgs[0]  = new CheckPoint.MsgDesc(msgId, finishedPartIndex, partNum);
		
		//write chk to log, if chk already in log, just update content, or append to log, and set it's offset;
		this.writeCheckPoint(chk);
	}

	private void writeCheckPoint(CheckPoint chk) throws IOException{
		if(chk.offsetInLog >= 0)
			this.fileAccess.seek(chk.offsetInLog);
		fileAccess.write(this.checkPoint2Bytes(chk));
		if(chk.offsetInLog < 0){
			chk.offsetInLog = fileAccess.getFilePointer() - CHECKPOINTSIZE;
		}
	}


	/**
	 * 检查消息体是否保存到了flume队列中，
	 * @param remoteQueue
	 * @param msgId
	 * @return	true：已经保存；false：没有
	 */
	public boolean checkIfBodySaved(String remoteQueue, String msgId) {
		CheckPoint chk = this.checkpoints.get(remoteQueue);
		if(chk != null && StringUtils.strEquals(msgId, chk.msgs[0].msgId)){
			return true;
		}else
			return false;
	}



	/**
	 * restore log to previous msg desc
	 * @param remoteQueue
	 */
	public void restoreLog(String remoteQueue) throws IOException{
		
		CheckPoint chk = this.checkpoints.get(remoteQueue);
		if(chk != null){
			chk.msgs[0] = chk.msgs[1];
			chk.msgs[1] = new CheckPoint.MsgDesc();
			this.writeCheckPoint(chk);
		}
	}

}
