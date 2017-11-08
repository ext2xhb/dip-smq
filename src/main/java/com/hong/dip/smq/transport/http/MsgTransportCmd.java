package com.hong.dip.smq.transport.http;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.Queue;
import com.hong.dip.utils.StringUtils;

public abstract class MsgTransportCmd {
	final static Logger log = LoggerFactory.getLogger(MsgTransportCmd.class);
	public static enum CmdType{
		MSG_POST, //POST A NEW MESSAGE PART
		MSG_POST_CONFIRM, //confirm to message post
		MSG_CHECK, //CHECK A MESSAGE 
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
	
	public static class MsgPostCmd extends MsgTransportCmd{
		private String msgId;
		private int partIndex; // msg part's index. -1: means body, others: 0, 1, 2 means attachments;
		private int chunkLen;
		private int chunkIndex; //start from 0;
		private Map<String, Integer> partNames;
		
		public MsgPostCmd(String msgId, int partIndex, int chunkLen, int chunkIndex){
			super(CmdType.MSG_POST);
			this.msgId = msgId;
			this.partIndex = partIndex;
			this.chunkLen = chunkLen;
			this.chunkIndex = chunkIndex;
		}
		public String getMsgId() {
			return msgId;
		}

		public int getPartIndex() {
			return partIndex;
		}

		public int getChunkLen() {
			return chunkLen;
		}

		public int getChunkIndex() {
			return chunkIndex;
		}
		
		public Map<String, Integer> getPartNames() {
			return partNames;
		}
		public void setPartNames(Map<String, Integer> partNames) {
			this.partNames = partNames;
		}
		@Override
		public void writeCmd(HeaderWriter writer) {
			writer.putHeader(HttpConstants.HEADER_ID, this.msgId);
			writer.putHeader(HttpConstants.HEADER_PART_IDX, Integer.toString(this.partIndex));
			writer.putHeader(HttpConstants.HEADER_CHUNK_IDX, Integer.toString(this.chunkIndex));
			writer.putHeader(HttpConstants.HEADER_CHUNK_LENGTH, Integer.toString(this.chunkLen));
			if(this.partNames != null && this.partNames.size() > 0){
				writer.putHeader(HttpConstants.HEADER_PART_NAMES, this.map2String(partNames));
			}
			
		}
		static private MsgPostCmd parseMsgPostCmd(HeaderReader request) {
			String msgId = request.getHeader(HttpConstants.HEADER_ID);
			if(StringUtils.isEmpty(msgId)){
				log.error("msg post cmd doesnt contains a valid msg id");
				return null;
			}
			int partIndex = StringUtils.parseInt(request.getHeader(HttpConstants.HEADER_PART_IDX),  -2);
			if(partIndex == -2){
				log.error("msg post cmd doesnt contains a valid partIndex" + request.getHeader(HttpConstants.HEADER_PART_IDX));
				return null;
			}
			int chunkIndex = StringUtils.parseInt(request.getHeader(HttpConstants.HEADER_CHUNK_IDX),  -1);
			if(chunkIndex == -1){
				log.error("msg post cmd doesnt contains a valid chunkIndex" + request.getHeader(HttpConstants.HEADER_CHUNK_IDX));
				return null;
			}
			int chunkLen = StringUtils.parseInt(request.getHeader(HttpConstants.HEADER_CHUNK_LENGTH),  -1);
			if(chunkIndex == -1){
				log.error("msg post cmd doesnt contains a valid chunk length" + request.getHeader(HttpConstants.HEADER_CHUNK_LENGTH));
				return null;
			}
			MsgPostCmd cmd = new MsgTransportCmd.MsgPostCmd(msgId, partIndex, chunkLen, chunkIndex);
			if(!StringUtils.isEmpty(request.getHeader(HttpConstants.HEADER_PART_NAMES))){
				Map<String, Integer> partNames = parseMapping(request.getHeader(HttpConstants.HEADER_PART_NAMES));
				cmd.setPartNames(partNames);
			}
			return cmd;
			
		}		
	}
	public static class MsgPostConfirm extends MsgTransportCmd{

		private HttpStatusException e;
		public MsgPostConfirm(){
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
		/**
		 * @param msgId
		 * @param partNum: 消息的part的总个数（最少为1）
		 * @param chunkSize：一个分片的大小。
		 */
		public MsgCheckCmd(String msgId, int partNum, int chunkSize){
			super(CmdType.MSG_CHECK);
			this.msgId = msgId;
			this.partNum = partNum;
			this.chunkSize = chunkSize;
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
			writer.putHeader(HttpConstants.HEADER_ID, msgId);
			writer.putHeader(HttpConstants.HEADER_PART_NUM, Integer.toString(partNum));
			writer.putHeader(HttpConstants.HEADER_CHUNK_SIZE, Integer.toString(chunkSize));
		}
		
		static private MsgCheckCmd parseMsgCheckCmd(HeaderReader request) {

			String msgId = request.getHeader(HttpConstants.HEADER_ID);
			if(StringUtils.isEmpty(msgId)){
				log.error("msg check cmd doesnt contains a valid msg id");
				return null;
			}
			int partNum = StringUtils.parseInt(request.getHeader(HttpConstants.HEADER_PART_NUM),  -1);
			if(partNum < 0){
				log.error("msg check cmd doesn't contains a valid partnum=" +  request.getHeader(HttpConstants.HEADER_PART_NUM));
				return null;
			}
			int chunkSize = StringUtils.parseInt(request.getHeader(HttpConstants.HEADER_CHUNK_SIZE), -1);
			if(chunkSize < 0){
				log.error("msg check cmd doesn't contains a valid chunksize=" +  request.getHeader(HttpConstants.HEADER_CHUNK_SIZE));
				return null;
			}
			return new MsgTransportCmd.MsgCheckCmd(msgId, partNum, chunkSize);
		}
	}
	
	public static class MsgCheckResult extends MsgTransportCmd{
		private String msgId;
		private int partIdx;
		private int chunkIdx;

		/**
		 * @param msgId: 消息id
		 * @param partIdx： 当前保存完整或者部分完整的消息Part序号。（小于该序号的Part一定要确保保存完整）
		 * @param chunkIndex：保存完整。无数据错误的seq序号；-1表示整个消息全部完整。
		 */
		public MsgCheckResult(String msgId, int partIdx, int chunkIdx){
			super(CmdType.MSG_CHECK);
			this.msgId = msgId;
			this.partIdx = partIdx;
			this.chunkIdx = chunkIdx;
		}

		public String getMsgId() {
			return msgId;
		}

		public int getPartIdx() {
			return partIdx;
		}

		public int getChunkIdx() {
			return chunkIdx;
		}

		@Override
		public void writeCmd(HeaderWriter writer) {
			writer.putHeader(HttpConstants.HEADER_ID, msgId);
			writer.putHeader(HttpConstants.HEADER_PART_IDX, Integer.toString(partIdx));
			writer.putHeader(HttpConstants.HEADER_CHUNK_IDX, Integer.toString(chunkIdx));
		
		}
		
	}

	public static MsgTransportCmd buildCmdForQueue(HttpServletRequest request, Queue queue) {
		String path = request.getContextPath();
		if(path.length() > 0 && path.charAt(path.length() - 1) == '/'){
			path = path.substring(0, path.length() - 1);
		}
		String qname = StringUtils.getResourceBase(path);
		if(queue.getName().equals(qname)){
			return parseCmd(request);
		}
		return null;
	}
	private static MsgTransportCmd parseCmd(HttpServletRequest request) {
		HeaderReader reader = new HeaderReader(){

			@Override
			public String getHeader(String header) {
				return request.getHeader(header);
			}
		};
		return parseCmd(reader);
	}
	

	public static MsgTransportCmd parseCmd(HeaderReader reader) {
		String cmd = reader.getHeader(HttpConstants.HEADER_CMD);
		if(HttpConstants.CMD_POST_MSG.equals(cmd)){
			return MsgPostCmd.parseMsgPostCmd(reader);
		}else if(HttpConstants.CMD_CHECK_MSG.equals(cmd)){
			return MsgCheckCmd.parseMsgCheckCmd(reader);
		}
		return null;	
	}

	public static String map2String(Map<String, Integer> names){
		StringBuilder builder = new StringBuilder();
		for(Map.Entry<String, Integer> set : names.entrySet()){
			builder.append(set.getKey()).append(':').append(set.getValue()).append(';');
		}
		return builder.toString();
	}
	public static Map<String, Integer> parseMapping(String s) {
		Map<String, Integer> names = new HashMap<String, Integer>();
		int start = 0;
		int occurs = -1;
		
		while(start < s.length()){
			occurs = s.indexOf(';', start);
			String part = null;
			if(occurs - start >  0){
				part = s.substring(start, occurs);
				
			}else if(occurs == -1){
				if(start < s.length()){
					part = s.substring(start);
				}
			}
			if(!splitNameValue(part, names)){
				break;
			}
			//continue next part ';'
			start = occurs + 1;
			if(occurs < 0)
				break;
		}
		if(names.size() == 0)
			return null;
		return names;
	}
	private static boolean splitNameValue(String part, Map<String, Integer> names) {
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
	public static interface HeaderReader{
		String getHeader(String header);
		
	}
	public static interface HeaderWriter{
		void putHeader(String header, String value);
	}
}
