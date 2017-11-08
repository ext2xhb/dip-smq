package com.hong.dip.smq.transport.http;

import java.nio.charset.Charset;

public interface HttpConstants {
	public static final int STATUS_OK = 200;
	public static final int STATUS_NOQUEUE=404; // not queue;
	public static final int STATUS_CONFLICT=409; //未知冲突，一般情况下应该不会出现。
	public static final int STATUS_UNAVAILABLE=406; //队列暂时不可用

	public static final String HEADER_CMD = "_smq_cmd";
	public static final String CMD_POST_MSG = "post";
	public static final String CMD_CHECK_MSG = "check";
	

	public static final String HEADER_ID = "_smq_id";
	public static final String HEADER_PART_NUM = "_smq_num"; //消息part的数量（每个Attachment是一个part, body也被当作一个特殊的part)
	public static final String HEADER_PART_IDX = "_smq_idx"; //part's index: -1:body, 0,1，2..: attachements parts;
	public static final String HEADER_CHUNK_LENGTH = "_smq_clen"; //当前 chunk的长度（字节数）
	public static final String HEADER_CHUNK_IDX = "_smq_cidx"; //message part的一个分片的序号: 0, 1,2,3 , 最后一个分片的序号为负数
	public static final String HEADER_CHUNK_SIZE = "_smq_csize"; //chunk size;
	
	public static final String HEADER_PART_NAMES = "_smq_names"; //attachment的名称列表（不包括body），格式为: name1;name2; ....
	public static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
	public static final String METHOD_POST = "POST";
	
	
}
