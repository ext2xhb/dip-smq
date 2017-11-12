package com.hong.dip.smq.transport.http;
public class HttpStatusException extends Exception{
	private static final long serialVersionUID = 4935772852119263862L;

	private int statusCode;

	public HttpStatusException(int statusCode){
		super("status error " + statusCode);
		this.statusCode = statusCode;
		
	}
	public HttpStatusException(int statusCode, String msg){
		super(msg + "status error " + statusCode);
		this.statusCode = statusCode;
		
	}
	public int getStatusCode(){
		return statusCode;
	}
}