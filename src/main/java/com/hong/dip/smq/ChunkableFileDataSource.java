package com.hong.dip.smq;

import java.io.File;
import java.io.IOException;

import javax.activation.FileDataSource;

public class ChunkableFileDataSource extends FileDataSource implements ChunkableDataSource{

	private String name;

	public ChunkableFileDataSource(String name, File file) {
		super(file);
		this.name = name;
	}
	public String getName(){
		return this.name;
	}
	@Override
	public long getSize() {
		return this.getFile().length();
	}

	@Override
	public void cleanSource() throws IOException {
		if(!this.getFile().delete()){
			throw new IOException(this.getFile() + " cannot be deleted");
		}
			
	}
	@Override
	public boolean isValid() {
		return this.getFile().isFile();
	}
	public String toString(){
		return this.getFile().toString();
	}
}
