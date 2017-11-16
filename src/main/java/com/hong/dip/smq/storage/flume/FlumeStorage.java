package com.hong.dip.smq.storage.flume;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.support.ServiceSupport;
import org.apache.flume.Context;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.storage.QueueStorage;
import com.hong.dip.smq.storage.Storage;
import com.hong.dip.utils.StringUtils;

public class FlumeStorage extends ServiceSupport implements Storage{
	final static Logger log = LoggerFactory.getLogger(FlumeStorage.class);
	private static final String ATTACHMENT_DIR = "attachment";
	
	
	ConcurrentHashMap<String, FlumeQueueStorage> queueStorages = new 
			ConcurrentHashMap<String, FlumeQueueStorage>();
	private FlumeOptions options;
	private File attachmentDir;
	
	
	public FlumeStorage(FlumeOptions options){
		this.options = options;
	}
	
	public File getStoragePath(){
		return new File(options.__getStoragePath());
	}
	
	private File getAttachmentDir() {
		if(this.attachmentDir == null){
			this.attachmentDir = new File(options.__getStoragePath(), ATTACHMENT_DIR);
		}
		return this.attachmentDir;
	}
	
	private void prepareStorageBase()  throws Exception{
		File dir = this.getStoragePath();
		StringUtils.ensureDirExists(dir);
		dir = this.getAttachmentDir();
		StringUtils.ensureDirExists(dir);
	}
	
	@Override
	public QueueStorage getOrCreateQueueStorage(String qname) throws Exception {
		FlumeQueueStorage  result = 
				queueStorages.putIfAbsent(qname, new FlumeQueueStorage(options, this.getQueueAttachmentDir(qname), qname, this.getStoragePath()));
		
		if(result == null){
			result = queueStorages.get(qname);
			try{
				result.start();
			}catch(Exception e){
				queueStorages.remove(qname);
				throw new Exception("cannot create queue " + qname, e);
			}
		}
		return result;
	}
	
	private File getQueueAttachmentDir(String qname) {
		//return new File(this.getAttachmentDir(), qname);
		return new File(new File(this.getStoragePath(), qname), ATTACHMENT_DIR);
	}
	@Override
	protected void doStop() throws Exception {
		for(FlumeQueueStorage queStore : queueStorages.values()){
			try{
				queStore.stop();
			}catch(Exception e){
				log.error("cannot stop queue storage " + queStore.getName(), e);
			}
		}
	}
	@Override
	protected void doStart() throws Exception {
		prepareStorageBase();
	}
	//TODO 以后实现
	@Override
	public void deleteQueueStorage(String name) {
		FlumeQueueStorage storage = queueStorages.remove(name);
		if(storage != null){
			try {
				storage.stop();
			} catch (Exception e) {
				log.error("failed to stop queue(" + storage.getName() + ")", e);
			}
			storage.delete();
		}
		
	}
}
