package com.hong.dip.smq.storage.flume;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.flume.Event;

import com.hong.dip.smq.ChunkableDataSource;

public class ChunkableEventDataSource implements ChunkableDataSource{
	private Event event;
	private String name;
	public ChunkableEventDataSource(String name, Event event){
		this.event = event;
		this.name = name;
	}
	@Override
	public String getContentType() {
		return "";
	}
	
	public Event getEvent(){
		return event;
	}
	@Override
	public InputStream getInputStream() throws IOException {
		return new ByteArrayInputStream(event.getBody());
	}

	@Override
	public String getName() {
		
		return name;
	}

	@Override
	public OutputStream getOutputStream() throws IOException {
		//TODO !!!-暂时用不着-!!!
		throw new IOException("unsupported");
	}

	@Override
	public long getSize() {
		return event.getBody() == null ? 0 : event.getBody().length;
	}

	@Override
	public void cleanSource() throws IOException {
		
	}

	@Override
	public boolean isValid() {
		return true;
	}
	@Override
	public ChunkReader openReader() throws IOException {
		return new EventReader();
	}
	class EventReader implements ChunkReader{

		@Override
		public ByteBuffer readChunk(ByteBuffer buffer) throws IOException {
			return ByteBuffer.wrap(event.getBody());
		}

		@Override
		public void close() {
				
		}

		@Override
		public void pos(long chunkIdx, int chunkSize) throws IOException {
			//nothing to do for event
		}

		@Override
		public long getSize() throws IOException {
			byte[] b = event.getBody();
			return b == null ? 0 : b.length;
		}
		
	}
}
