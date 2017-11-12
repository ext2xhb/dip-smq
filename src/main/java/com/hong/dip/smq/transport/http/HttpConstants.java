package com.hong.dip.smq.transport.http;

import java.nio.charset.Charset;

public interface HttpConstants {
	public static final int STATUS_OK = 200;
	public static final int STATUS_NOQUEUE=404; //队列不可用 
	public static final int STATUS_CONFLICT=409; //未知冲突，一般情况下应该不会出现。一旦出现表示无法继续处理当前消息
	public static final int STATUS_UNAVAILABLE=503; //不可用错误（可以重试的一般性错误）

	public static final String HEADER_CMD = "QCMD";
	public static final String CMD_SEND_MSG = "SNDMSG";
	public static final String CMD_CHECK_MSG = "CHK";
	public static final String CMD_CHECK_MSG_RESULT = "CHK_R";
	

	public static final String HEADER_SENDER_QUEUE = "SNDQUE";
	public static final String HEADER_MSG_ID = "MSGID";
	public static final String HEADER_PART_NUM = "PARTNUM"; //消息part的数量（每个Attachment是一个part, body也被当作一个特殊的part)
	public static final String HEADER_PART_IDX = "PARTIDX"; //part's index: -1:body, 0,1，2..: attachements parts;
	public static final String HEADER_PART_LENGTH = "PARTLEN"; //当前 chunk的长度（字节数）
	public static final String HEADER_CHUNK_LENGTH = "CHUNKLEN"; //当前 chunk的长度（字节数）
	public static final String HEADER_CHUNK_IDX = "CHUNKIDX"; //message part的一个分片的序号: 0, 1,2,3 , 最后一个分片的序号为负数
	public static final String HEADER_CHUNK_SIZE = "CHUNKSIZE"; //chunk 分片的大小（一般是一个配置的固定值）;
	
	public static final String HEADER_ATTACHMENT_NAMES = "ATNAMES"; //attachment的名称列表（不包括body），格式为: name1;name2; ....
	public static final String HEADER_ATTACHMENT_PATHS = "ATPATHS"; //attachment的名称列表（不包括body），格式为: name1;name2; ....
	public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
	//public static final String METHOD_POST = "POST";
	public static final String HEADER_QNAME = "q_qname";
	public static final String HEADER_CONTENT_LENGTH = "Content-Length";
	
	
}
