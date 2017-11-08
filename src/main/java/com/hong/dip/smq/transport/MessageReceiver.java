package com.hong.dip.smq.transport;

import com.hong.dip.smq.Message;

public interface MessageReceiver {
	public void onMessage(Message message) throws Exception;
}
