package com.hong.dip.smq.transport.http;
public class HttpStatusException extends Exception{
	private static final long serialVersionUID = 4935772852119263862L;

	private int statusCode;

	public HttpStatusException(int statusCode, String description){
		super(description);
		this.statusCode = statusCode;
		
	}
	public int getStatusCode(){
		return statusCode;
	}
}