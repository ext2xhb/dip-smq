package com.hong.dip.smq;

public class Node {
	private String name;
	private String ip;
	private int port;
	
	public Node(String name, String ip, int port){
		this.name = name;
		this.ip = ip;
		this.port = port;
	}
	public String getName(){
		return this.name;
	}
	public String getIp() {
		return ip;
	}
	public int getPort() {
		return port;
	}
	
}
