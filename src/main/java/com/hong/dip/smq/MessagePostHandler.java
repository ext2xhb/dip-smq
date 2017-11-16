package com.hong.dip.smq;

import java.util.List;

/**
 * 消息发送完毕后的后置操作
 */
public interface MessagePostHandler {
	//
	/**
	 * Handler是否需要消息的完整内容。
	 * 死信的情况下总是返回消息的完整内容;但是正常结束的情况下，needFullMessage()返回True，才通知完整的消息内容。
	 * false只通知消息id和附件列表
	 * @return true：告诉消息组件，通知完整的消息内容。false：告诉消息组件，不通知完整消息内容（只有id和附件）
	 */
	public boolean needFullMessage();
	/**
	 * @param reason: 结束的原因；finished
	 * @param msgId
	 * @param attachments
	 */
	public void handle(MessageReason reason, Message msg);
}
