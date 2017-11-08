package com.hong.dip.smq;

import java.io.File;

public class  FileAttachment {
	private String name;
	private File file;

	public FileAttachment(String name, File file){
		this.name = name;
		this.file = file;
	}
	public String getName(){
		return name;
	}
	public File getFile(){
		return file;
	}
}
