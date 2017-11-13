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
	private static final byte[] ZEROBYTES = new byte[0];
	private String name;
	private long fileLength;

	public ChunkableFileDataSource(String name, File file) {
		super(file);
		this.name = name;
		fileLength = this.getFile().length();
	}
	public String getName(){
		return this.name;
	}
	@Override
	public long getSize() {
		return fileLength;
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
		long lastChunkOffset = 0;
		boolean isEmptyFile = false;
		boolean emptyReaded = false;
		FileChunkReader() throws IOException{
			file = new RandomAccessFile(getFile(), "r");
			channel = file.getChannel();
			isEmptyFile = channel.size() == 0;
		}
		
		@Override
		public ByteBuffer readChunk(ByteBuffer buffer) throws IOException {
			if(this.isEmptyFile){
				return readChunkOfEmptyFile();
			}
			long pos = channel.position();
			buffer.clear();
			int left = buffer.capacity();
			while(left > 0){
				int readed = channel.read(buffer);
				if(readed < 0)
					break;
				left -= readed;
			}
			//if read something
			if(left < buffer.capacity()){
				buffer.flip();
				this.lastChunkOffset = pos;
				return buffer;
			}else //read nothing
				return null;
		}

		private ByteBuffer readChunkOfEmptyFile() {
			if(!this.emptyReaded){
				this.emptyReaded = true;
				return ByteBuffer.wrap(ZEROBYTES);
			}
			else
				return null;
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
			if(pos > channel.size()) /*????*/
				throw new IOException("Exceed file("+getFile()+") 's size("+channel.size()+") but seek to " + pos);
			channel.position(pos);
		}

		@Override
		public long getSize() throws IOException {
			//return channel.size();
			return fileLength;
		}

		@Override
		public long getReadedChunkOffset() {
			return this.lastChunkOffset;
		}
		
	}
}
