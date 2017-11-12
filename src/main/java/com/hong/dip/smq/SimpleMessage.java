package com.hong.dip.smq;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleMessage implements Message {
	final static Logger log = LoggerFactory.getLogger(SimpleMessage.class);
	String msgId;
	byte[] byteBody;
	List<String> attachmentNames = new ArrayList<String>();
	List<ChunkableDataSource> attachments = new ArrayList<ChunkableDataSource>();
	
	
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#setID(java.lang.String)
	 */
	public void setID(String msgId){
		this.msgId = msgId;
	}
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getID()
	 */
	public String getID() {
		return msgId;
	}

	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getByteBody()
	 */
	public byte[] getByteBody() {
		return byteBody;
	}

	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#setByteBody(byte[])
	 */
	public void setByteBody(byte[] byteBody) {
		this.byteBody = byteBody;
	}

	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#addAttachment(com.hong.dip.smq.ChunkableDataSource)
	 */
	public boolean addAttachment(ChunkableDataSource attachment){
		if(!attachment.isValid()){
			log.error("attachment ("+attachment+") is not a valid");
			return false;
		}
		attachments.add(attachment);
		attachmentNames.add(attachment.getName());
		return true;
	}
	
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getAttachment(java.lang.String)
	 */
	public ChunkableDataSource getAttachment(String name){
		for(ChunkableDataSource src : attachments){
			if(name.equals(src.getName())){
				return src;
			}
		}
		return null;
	}


	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getAttachmentNameList()
	 */
	public List<String> getAttachmentNameList() {
		return attachmentNames;
	}
	/* (non-Javadoc)
	 * @see com.hong.dip.smq.Message#getAttachments()
	 */
	public List<ChunkableDataSource> getAttachments(){
		return this.attachments;
	}
	
	//Message Storage Functions

}
