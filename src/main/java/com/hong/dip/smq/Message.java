package com.hong.dip.smq;

import java.io.File;
import java.io.IOException;
import java.util.List;

public interface Message {

	void setID(String msgId);
	String getID();
	byte[] getByteBody();

	void setByteBody(byte[] byteBody);

	/**
	 * @param attachment
	 * @return false: 没成功；Datasource不正确会导致失败。true: 成功
	 */
	void addAttachment(ChunkableDataSource attachment) throws IOException;
	void addAttachment(String name, File attachment) throws IOException;
	List<ChunkableDataSource> getAttachments();

	ChunkableDataSource getAttachment(String name);
	List<String> getAttachmentNameList();


}