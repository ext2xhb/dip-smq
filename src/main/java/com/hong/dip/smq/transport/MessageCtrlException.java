package com.hong.dip.smq.transport;

/**
 * @author xuhb
 * 这里的异常不是通常意义上的Exception。而是用来描述消息是否发生了不可恢复的异常还是临时的异常。
 * 消息的处理流程可以以此为依据进行重试或者标记为死信（目前版本暂不支持死信，只是简单输出日志）
 */
public class MessageCtrlException {
	//means recoverable exception, we can rollback and redeal with such message;
	static public class TemporaryMessageException extends Exception{
		private static final long serialVersionUID = 1L;

		public TemporaryMessageException(String msg, Throwable cause){
			super(msg, cause);
		}
	}
	//fatal exception: means unrecoverable exception, current message can only be discarded or dead queue(unsupported now)
	static public class FatalMessageException extends Exception{
		private static final long serialVersionUID = 1L;
		public FatalMessageException(String msg, Throwable cause){
			super(msg, cause);
		}
	}

}
