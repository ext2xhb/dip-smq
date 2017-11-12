package com.hong.dip.smq.transport.http;

/**
 * 当解析Http协议中传递的CMD时发现失败，则抛出异常。一般情况下不可能出现此异常（除非程序有Bug）
 * 目前此异常被当作普通的网络异常处理（即：被当作可恢复的异常进行处理）
 */
public class HttpInvalidCmdException extends HttpNetException {
	private static final long serialVersionUID = -6529218163270823929L;

	public HttpInvalidCmdException(String msg, Class<?> cmdClass) {
		super("cannot create cmd " + cmdClass + " details is" + msg, null);
	}

}
