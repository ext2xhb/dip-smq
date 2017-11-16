package com.hong.dip.smq;

public enum MessageReason {
	Finished, //消息发送正常结束
	Dead_InvalideContent, //消息内容不正确导致的死信，多数是因为附件无法读取
	Dead_Other //其他原因导致的死信
}
