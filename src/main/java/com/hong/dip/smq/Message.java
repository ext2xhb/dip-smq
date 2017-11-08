package com.hong.dip.smq;

import java.util.List;

public interface Message {

	byte[] getByteBody();

	void setByteBody(byte[] byteBody);

	void addAttachment(ChunkableDataSource attachment) throws Exception;

	ChunkableDataSource getAttachment(int idx);

	int getAttachmentNum();

	ChunkableDataSource getAttachment(String name);

	String getID();
	
	List<String> getAttachmentNameList();
}