package com.hong.dip.smq.transport.http;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.storage.QueueStorage;
import com.hong.dip.utils.StringUtils;

public abstract class MsgTransportCmd {
	final static Logger log = LoggerFactory.getLogger(MsgTransportCmd.class);
	public static enum CmdType{
		MSG_POST_CHUNK, //POST A NEW MESSAGE PART
		MSG_POST_CONFIRM, //confirm to message post
		MSG_CHECK_MSG, //CHECK A MESSAGE 
		MSG_CHECK_RESULT,

	}

	private CmdType type;
	
	MsgTransportCmd(CmdType type){
		this.type = type;
	}
	public CmdType getCmdType(){
		return type;
	}
	public abstract void writeCmd(HeaderWriter writer);
	
	/**
	 *	发送消息内容的命令，该命令的http request body内容为消息的内容
	 */
	public static class MsgPostCmd extends MsgTransportCmd{
		private String msgId;	//msg id 
		private int partIndex; // msg part's index. -1: means body, others: 0, 1, 2 means attachments;
		private int chunkLen;  //current chunk's length
		private String attachmentNames; //attachment names of this message;
		//private String attachmentPaths;
		private int partNum;
		private long partLength;
		private long chunkOffset;
		private String senderQueue;
		
		public String getAttachmentNames() {
			return attachmentNames;
		}
		public void setAttachmentNames(String attachmentNames) {
			this.attachmentNames = attachmentNames;
		}
		/**
		 * @param msgId: 当前消息的ID
		 * @param partNum：当前消息一共有多少part
		 * @param partIndex: 当前发送的chunk所在的part的idx
		 * @param partLength：当前part的总长度（字节数)
		 * @param chunkLen： 当前分片的长度
		 */
		public MsgPostCmd(String senderQueue, String msgId, int partNum, int partIndex, long partLength, long chunkOffset, int chunkLen){
			super(CmdType.MSG_POST_CHUNK);
			this.senderQueue = senderQueue;
			this.msgId = msgId;
			this.partNum = partNum;
			this.partIndex = partIndex;
			this.partLength = partLength;
			this.chunkOffset = chunkOffset;
			this.chunkLen = chunkLen;
		}
		public String getSenderQueue() {
			return this.senderQueue;
		}		
		
		public String getMsgId() {
			return msgId;
		}

		public int getPartNum() {
			return partNum;
		}
		public int getPartIndex() {
			return partIndex;
		}

		public long getPartLength() {
			return partLength;
		}

		public int getChunkLen() {
			return chunkLen;
		}
		public long getChunkOffset() {
			return this.chunkOffset;
		}

		
		@Override
		public void writeCmd(HeaderWriter writer) {
			writer.putHeader(HttpConstants.HEADER_CMD,  HttpConstants.CMD_SEND_MSG);
			
			writer.putHeader(HttpConstants.HEADER_MSG_ID, this.msgId);
			writer.putHeader(HttpConstants.HEADER_SENDER_QUEUE, this.senderQueue);
			writer.putIntHeader(HttpConstants.HEADER_PART_NUM, this.partNum);
			writer.putIntHeader(HttpConstants.HEADER_PART_IDX, this.partIndex);
			writer.putLongHeader(HttpConstants.HEADER_PART_LENGTH, this.partLength);
			writer.putIntHeader(HttpConstants.HEADER_CHUNK_LENGTH, this.chunkLen);
			writer.putLongHeader(HttpConstants.HEADER_CHUNK_OFFSET, this.chunkOffset);
			
			if(attachmentNames != null){
				writer.putHeader(HttpConstants.HEADER_ATTACHMENT_NAMES, attachmentNames);
			}
		}
		private static MsgTransportCmd parseMsgPostCmd(HeaderReader request) throws HttpInvalidCmdException{
			String senderQueue= request.getHeader(HttpConstants.HEADER_SENDER_QUEUE);
			if(StringUtils.isEmpty(senderQueue)){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO SENDER QUEUE", MsgPostCmd.class);
				log.error(e.toString(), e);
				throw e;
			}

			String msgId = request.getHeader(HttpConstants.HEADER_MSG_ID);
			if(StringUtils.isEmpty(msgId)){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO Message ID", MsgPostCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			int partNum = request.getIntHeader(HttpConstants.HEADER_PART_NUM, Integer.MIN_VALUE);
			if(partNum == Integer.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO PART NUM", MsgPostCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			int partIndex =request.getIntHeader(HttpConstants.HEADER_PART_IDX,  Integer.MIN_VALUE);
			if(partIndex == Integer.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO PART IDX", MsgPostCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			long partLen = request.getLongHeader(HttpConstants.HEADER_PART_LENGTH,  Long.MIN_VALUE);
			if(partLen == Long.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO PART LENGTH", MsgPostCmd.class);
				log.error(e.toString(), e);
				throw e;
			}

			long chunkOffset = request.getLongHeader(HttpConstants.HEADER_CHUNK_OFFSET,  Long.MIN_VALUE);
			if(chunkOffset == Long.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO CHUNK OFFSET", MsgPostCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			
			int chunkLen = request.getIntHeader(HttpConstants.HEADER_CHUNK_LENGTH,  Integer.MIN_VALUE);
			if(chunkLen == Integer.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO CHUNK LENGTH", MsgPostCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			MsgPostCmd cmd = new MsgTransportCmd.MsgPostCmd(senderQueue, msgId, partNum, partIndex, partLen, chunkOffset, chunkLen);
			String attachmentNames = request.getHeader(HttpConstants.HEADER_ATTACHMENT_NAMES);
			if(attachmentNames != null ){
				cmd.setAttachmentNames(attachmentNames);
			}
			return cmd;
			
		}
	}
	public static class MsgSuccessConfirm extends MsgTransportCmd{

		private HttpStatusException e;
		public MsgSuccessConfirm(){
			super(CmdType.MSG_POST_CONFIRM);
		}
		
		public void setException(HttpStatusException e){
			this.e = e;
		}
		public HttpStatusException getException(){
			return this.e;
		}
		
		public int getHttpStatus(){
			if(e == null)
				return HttpConstants.STATUS_OK;
			else
				return e.getStatusCode();
		}

		@Override
		public void writeCmd(HeaderWriter writer) {
			
		}
		
	}
	
	public static class MsgCheckCmd extends MsgTransportCmd{
		private String msgId;
		private int partNum;
		private int chunkSize;
		private String senderQueueName;
		/**
		 * @param senderQueueName: 发送队列名
		 * @param msgId
		 * @param partNum: 消息的part的总个数（由于Body也是一个Part，所以个数最少为1）
		 * @param chunkSize：一个分片的大小。
		 */
		public MsgCheckCmd(String senderQueueName, String msgId, int partNum, int chunkSize){
			super(CmdType.MSG_CHECK_MSG);
			this.msgId = msgId;
			this.partNum = partNum;
			this.chunkSize = chunkSize;
			this.senderQueueName = senderQueueName;
		}
		
		public String getSenderQueueName(){
			return this.senderQueueName;
		}
		public String getMsgId() {
			return msgId;
		}

		public int getPartNum() {
			return partNum;
		}

		public int getChunkSize() {
			return chunkSize;
		}
		@Override
		public void writeCmd(HeaderWriter writer) {
			writer.putHeader(HttpConstants.HEADER_CMD, HttpConstants.CMD_CHECK_MSG);
			writer.putHeader(HttpConstants.HEADER_QNAME, this.senderQueueName);
			writer.putHeader(HttpConstants.HEADER_MSG_ID, msgId);
			writer.putHeader(HttpConstants.HEADER_PART_NUM, Integer.toString(partNum));
			writer.putHeader(HttpConstants.HEADER_CHUNK_SIZE, Integer.toString(chunkSize));
		}
		
		static private MsgCheckCmd parseMsgCheckCmd(HeaderReader request) throws HttpInvalidCmdException {
			String senderQName = request.getHeader(HttpConstants.HEADER_QNAME);
			if(StringUtils.isEmpty(senderQName)){
				HttpInvalidCmdException e = new HttpInvalidCmdException("No QName", MsgCheckCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			String msgId = request.getHeader(HttpConstants.HEADER_MSG_ID);
			if(StringUtils.isEmpty(msgId)){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO Message ID", MsgCheckCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			int partNum = request.getIntHeader(HttpConstants.HEADER_PART_NUM, Integer.MIN_VALUE);
			if(partNum == Integer.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO Part Num", MsgCheckCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			int chunkSize = request.getIntHeader(HttpConstants.HEADER_CHUNK_SIZE, Integer.MIN_VALUE);
			if(chunkSize == Integer.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO CHUNK SIZE", MsgCheckCmd.class);
				log.error(e.toString(), e);
				throw e;
			}
			
			return new MsgTransportCmd.MsgCheckCmd(senderQName, msgId, partNum, chunkSize);
		}
	}
	
	public static class MsgCheckResult extends MsgTransportCmd{
		private int partIdx;
		private long chunkIdx;

		/**
		 * @param msgId: 消息id
		 * @param partIdx： 当前保存完整或者部分完整的消息Part序号。小于该序号的Part的内容是完整的，所以只需要从当前partIndex开始保存即可。
		 * @param chunkIndex：>=0表示需要从当前chunkIndex重新写；-1表示整个消息全部完整，无需继续写
		 */
		public MsgCheckResult(int partIdx, long chunkIdx){
			super(CmdType.MSG_CHECK_RESULT);
			this.partIdx = partIdx;
			this.chunkIdx = chunkIdx;
		}

		public int getPartIdx() {
			return partIdx;
		}

		public long getChunkIdx() {
			return chunkIdx;
		}

		@Override
		public void writeCmd(HeaderWriter writer) {
			writer.putHeader(HttpConstants.HEADER_CMD, HttpConstants.CMD_CHECK_MSG_RESULT);
			
			writer.putHeader(HttpConstants.HEADER_PART_IDX, Integer.toString(partIdx));
			writer.putHeader(HttpConstants.HEADER_CHUNK_IDX, Long.toString(chunkIdx));
		
		}

		static MsgTransportCmd parseMsgCheckResult(HeaderReader reader) throws HttpInvalidCmdException {
			int partIdx = reader.getIntHeader(HttpConstants.HEADER_PART_IDX, Integer.MIN_VALUE);
			if(partIdx == Integer.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO PART IDX", MsgCheckResult.class);
				log.error(e.toString(), e);
				throw e;
			}
			long chunkIdx = reader.getLongHeader(HttpConstants.HEADER_CHUNK_IDX, Long.MIN_VALUE);
			if(chunkIdx == Long.MIN_VALUE){
				HttpInvalidCmdException e = new HttpInvalidCmdException("NO CHUNK IDX", MsgCheckResult.class);
				log.error(e.toString(), e);
				throw e;
			}
			return new MsgCheckResult(partIdx, chunkIdx); 
		}
		
	}

	public static MsgTransportCmd parseCmd(HttpServletRequest request, QueueStorage queue) throws HttpInvalidCmdException {
		String path = request.getContextPath();
		if(path.length() > 0 && path.charAt(path.length() - 1) == '/'){
			path = path.substring(0, path.length() - 1);
		}
		String qname = StringUtils.getResourceBase(path);
		if(queue.getName().equals(qname)){
			return parseCmd(request);
		}
		else
			throw new HttpInvalidCmdException("Received a request, But NO Matched Queue request is" + path, null);
	}
	private static MsgTransportCmd parseCmd(HttpServletRequest request) throws HttpInvalidCmdException {
		HeaderReader reader = new HeaderReader(){

			@Override
			public String getHeader(String header) {
				return request.getHeader(header);
			}
		};
		return parseCmd(reader);
	}
	

	public static MsgTransportCmd parseCmd(HeaderReader reader) throws HttpInvalidCmdException{
		String cmd = reader.getHeader(HttpConstants.HEADER_CMD);
		if(HttpConstants.CMD_SEND_MSG.equals(cmd)){
			return MsgPostCmd.parseMsgPostCmd(reader);
		}else if(HttpConstants.CMD_CHECK_MSG.equals(cmd)){
			return MsgCheckCmd.parseMsgCheckCmd(reader);
		}else if(HttpConstants.CMD_CHECK_MSG_RESULT.equals(cmd)){
			return MsgCheckResult.parseMsgCheckResult(reader);
		}else if(cmd == null)
			return new MsgTransportCmd.MsgSuccessConfirm();
		else
			throw new HttpInvalidCmdException("UNKNOWN CMD " + cmd, null);
		
	}
	public static abstract class HeaderReader{
		public abstract String getHeader(String name);
		
		public long getLongHeader(String name, long defaultValue) {
			try{
				return Long.parseLong(getHeader(name));
			}catch(NumberFormatException e){
				return defaultValue;
			}
		}
		
		public int getIntHeader(String name, int defaultValue){
			try{
				return Integer.parseInt(getHeader(name));
			}catch(NumberFormatException e){
				return defaultValue;
			}
		}
	}
	public static abstract class  HeaderWriter {
		public abstract void putHeader(String name, String value);
		public void putIntHeader(String name, int value){
			putHeader(name, Integer.toString(value));
		}
		public void putLongHeader(String name, long value){
			putHeader(name, Long.toString(value));
		}
	}
	/*
	public static boolean splitNameValue(String part, Map<String, Integer> names) {
		if(part == null)
			return false;
		String name, value;
		
		int pos = part.indexOf(':');
		if(pos > 0){
			name = part.substring(0,  pos); //not empty
			value = part.substring(pos+1);
			try{
				int v = Integer.parseInt(value);
				names.put(name, v);
				return true;
			}catch(Exception e){
				log.error("part (" + part + ") cannot be parsed a string:int");
			}
		}
		return false;
	}
	*/

}
