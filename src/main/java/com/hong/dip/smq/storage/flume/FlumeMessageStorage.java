package com.hong.dip.smq.storage.flume;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.ChunkableDataSource;
import com.hong.dip.smq.ChunkableFileDataSource;
import com.hong.dip.smq.Message;
import com.hong.dip.smq.SimpleMessage;
import com.hong.dip.smq.storage.MessageStorage;
import com.hong.dip.utils.StringUtils;

public class FlumeMessageStorage implements MessageStorage {
	final static Logger log = LoggerFactory.getLogger(FlumeMessageStorage.class);
	private static final String HEADER_ID = "id";
	private static final String HEADER_ATTACHMENT_NAMELIST = "names";
	private static final String HEADER_ATTACHMENT_PATH = "paths";
	private static final String HEADER_SEQUENCE = "seq";
	
	static MessageStorage cast2Storage(Message m){
		FlumeMessageStorage storage = new FlumeMessageStorage();
		
		Event event = storage.getEvent();
		Map<String, String> headers = event.getHeaders();
		//set id && body
		headers.put(HEADER_ID, m.getID());
		event.setBody(m.getByteBody());
		
		//set attachment if we has
		StringBuilder names = new StringBuilder();
		StringBuilder paths = new StringBuilder();
		for(ChunkableDataSource ds : m.getAttachments()){
			names.append(ds.getName()).append(';');
			paths.append(((ChunkableFileDataSource)ds).getFile().getPath()).append(';');
			storage.addPart(ds);
		}
		if(names.length() > 0){
			headers.put(HEADER_ATTACHMENT_NAMELIST, names.toString());
			headers.put(HEADER_ATTACHMENT_PATH, paths.toString());
		}
		return storage;
	}
	
	static Message cast2Message(MessageStorage storage) {
		Message m = new SimpleMessage();
		m.setID(storage.getID());
		m.setByteBody(((FlumeMessageStorage)storage).getEvent().getBody());
		
		List<ChunkableDataSource> parts = storage.getParts();
		for(int i = 0; i < parts.size() - 1; i++)
			try{
				m.addAttachment(parts.get(i));
			}catch(Exception e){
				//impossible
				log.error("attachment("+parts.get(i)+") of storage is not valid", e);
			}
		return m;
	}


	ChunkableEventDataSource body;
	List<ChunkableDataSource> parts = new ArrayList<ChunkableDataSource>(2);
	private long storeSequence;
	
	public FlumeMessageStorage(){
		body = new ChunkableEventDataSource("body", new SimpleEvent());
		parts.add(body);
		
	}
	
	public FlumeMessageStorage(Event event) {
		body = new ChunkableEventDataSource("body", event);
		parts.add(body);
		this.makeAttachments();
	}
	public Event getEvent(){
		return body.getEvent();
	}
	@Override
	public String getID() {
		return getEvent().getHeaders().get(HEADER_ID);
	}
	@Override
	public void setID(String msgId) {
		 getEvent().getHeaders().put(HEADER_ID, msgId);
	}



	public String getAttachmentNames(){
		return getEvent().getHeaders().get(HEADER_ATTACHMENT_NAMELIST);
	}
	
	public void setAttachmentNames(List<String> names){
		if(names != null  && names.size() > 0){
			StringBuilder s = new StringBuilder();
			for(String name : names){
				s.append(name).append(';');
			}
			getEvent().getHeaders().put(HEADER_ATTACHMENT_NAMELIST, s.toString());
		}
		
	}
	public void setAttachmentPaths(List<String> paths) {
		if(paths != null  && paths.size() > 0){
			StringBuilder s = new StringBuilder();
			for(String path : paths){
				s.append(path).append(';');
			}
			getEvent().getHeaders().put(HEADER_ATTACHMENT_PATH, s.toString());
		}
		
	}
//
//	public String getAttachmentPath(){
//		return getEvent().getHeaders().get(HEADER_ATTACHMENT_PATH);
//	}
	
	@Override
	public long getStoreSequence() {
		return storeSequence;
	}

	@Override
	public void setStoreSequence(long sequence) {
		getEvent().getHeaders().put(HEADER_SEQUENCE, Long.toString(sequence));
		this.storeSequence = sequence;
	}

	@Override
	public <T> T getBodyContent(Class<T> c) {
		return (T) getEvent();
	}

	private void makeAttachments(){
		String paths = getEvent().getHeaders().get(HEADER_ATTACHMENT_PATH);
		String names = getEvent().getHeaders().get(HEADER_ATTACHMENT_NAMELIST);
		if(paths != null ){
			List<String> pathlist = StringUtils.string2List(paths);
			List<String> namelist = StringUtils.string2List(names);
			
			int size = Math.min(pathlist.size(), namelist.size()); //TODO : 只是为了防范未知的意外，导致程序崩溃 !!!!不应该出现pathlist, namelist不同的情况
			for(int i = 0; i < size; i++){
				ChunkableDataSource ds = new ChunkableFileDataSource(namelist.get(i), new File(pathlist.get(i)));
				this.addPart(ds);
			}
			
		}
	}
	
	@Override
	public void addPart(ChunkableDataSource ds) {
		this.parts.add(null);
		this.parts.set(this.parts.size() - 2 , ds);
		this.parts.set(this.parts.size() - 1, this.body);
		
	}

	@Override
	public List<ChunkableDataSource> getParts() {
		return this.parts;
	}



}
