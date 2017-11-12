package com.hong.dip.smq;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import javax.activation.FileDataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkableFileDataSource extends FileDataSource implements ChunkableDataSource{
	static final private Logger log = LoggerFactory.getLogger(ChunkableFileDataSource.class);
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
	@Override
	public ChunkReader openReader() throws IOException {
		return new FileChunkReader();
	}
	
	class FileChunkReader implements ChunkReader{
		FileChannel channel;
		RandomAccessFile file;
		FileChunkReader() throws IOException{
			file = new RandomAccessFile(getFile(), "r");
			channel = file.getChannel();
		}
		
		@Override
		public ByteBuffer readChunk(ByteBuffer buffer) throws IOException {
			buffer.clear();
			channel.read(buffer);
			buffer.flip();
			return buffer;
		}

		@Override
		public void close() {
			try {
				channel.close();
			} catch (IOException e) {
				log.error("failed close channel of file(" + getFile()+")", e);
			}
			try {
				file.close();
			} catch (IOException e) {
				log.error("failed close file(" + getFile()+")", e);
			}
		}

		@Override
		public void pos(long chunkIdx, int chunkSize) throws IOException{
			long pos = chunkIdx * chunkSize;
			if(pos >= channel.size())
				throw new IOException("Exceed file("+getFile()+") 's size("+channel.size()+") but seek to " + pos);
			channel.position(pos);
		}

		@Override
		public long getSize() throws IOException {
			return channel.size();
		}
		
	}
}
