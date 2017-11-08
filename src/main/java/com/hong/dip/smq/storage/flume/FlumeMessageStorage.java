package com.hong.dip.smq.storage.flume;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.hong.dip.smq.ChunkableDataSource;
import com.hong.dip.smq.Message;
import com.hong.dip.smq.storage.MessageStorage;

public class FlumeMessageStorage implements Message, MessageStorage {
	String msgId;

	byte[] byteBody;
	
	List<String> attachmentNames = new ArrayList<String>();
	List<ChunkableDataSource> attachments = new ArrayList<ChunkableDataSource>();
	
	FlumeMessageStorage(){
		
	}
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getByteBody()
	 */
	@Override
	public byte[] getByteBody() {
		return byteBody;
	}

	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#setByteBody(byte[])
	 */
	@Override
	public void setByteBody(byte[] byteBody) {
		this.byteBody = byteBody;
	}
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#addAttachment(com.hong.dip.smq.ChunkableDataSource)
	 */
	@Override
	public void addAttachment(ChunkableDataSource attachment) throws Exception{
		if(!attachment.isValid()){
			throw new IOException("attachment ("+attachment+") is not a valid");
		}
		attachments.add(attachment);
		attachmentNames.add(attachment.getName());
	}
	
	void _addAttachment(ChunkableDataSource attachment){
		attachments.add(attachment);
		attachmentNames.add(attachment.getName());
	}
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getAttachment(int)
	 */
	@Override
	public ChunkableDataSource getAttachment(int idx){
		return attachments.get(idx);
	}
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getAttachmentNum()
	 */
	@Override
	public int getAttachmentNum(){
		return attachments.size();
	}
	
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getAttachment(java.lang.String)
	 */
	@Override
	public ChunkableDataSource getAttachment(String name){
		for(ChunkableDataSource src : attachments){
			if(name.equals(src.getName())){
				return src;
			}
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getID()
	 */
	@Override
	public String getID() {
		return msgId;
	}
	@Override
	public List<String> getAttachmentNameList() {
		return attachmentNames;
	}
	
	
	//Message Storage Functions
	
	@Override
	public void updateID(String id) {
		this.msgId = id;
	}
}
