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
	private static final String BACKUP_DIR = "backup";
	private static final String CHECKPOINT_DIR = "chk";
	private static final String DATA_DIR = "data";
	private static final String ATTACHMENT_DIR = "attachment";
	
	Context context;
	ConcurrentHashMap<String, FlumeQueueStorage> queueStorages = new 
			ConcurrentHashMap<String, FlumeQueueStorage>();
	private FlumeOptions options;
	private File attachmentDir;
	
	public FlumeStorage(FlumeOptions options){
		this.options = options;
		this.attachmentDir = new File(options.getStoragePath(), ATTACHMENT_DIR);
	}
	private Context createContext(FlumeOptions options) {
		Context context = new Context();
		context.put(FileChannelConfiguration.CHECKPOINT_DIR, this.getCheckPointDir(options).getPath());
		context.put(FileChannelConfiguration.BACKUP_CHECKPOINT_DIR,
			this.getBackupDir(options).getPath());
		context.put(FileChannelConfiguration.DATA_DIRS, this.getDataDir(options).getPath());
		context.put(FileChannelConfiguration.KEEP_ALIVE, String.valueOf(options.getPutWaitSeconds()));
		context.put(FileChannelConfiguration.CAPACITY, String.valueOf(options.getDefaultQueueDepth()));
		context.put(FileChannelConfiguration.TRANSACTION_CAPACITY,String.valueOf(options.getTransactionCapacity()));
		
		return context;

		
	}
	private File getBackupDir(FlumeOptions options) {
		return new File(options.getStoragePath(), BACKUP_DIR);
	}
	private File getCheckPointDir(FlumeOptions options) {
		return new File(options.getStoragePath(), CHECKPOINT_DIR);
	}
	private File getDataDir(FlumeOptions options) {
		return new File(options.getStoragePath(), DATA_DIR);
	}
	private File getAttachmentDir() {
		if(this.attachmentDir == null){
			this.attachmentDir = new File(options.getStoragePath(), ATTACHMENT_DIR);
		}
		return this.attachmentDir;
	}
	
	private void prepareStorageBase(FlumeOptions options)  throws Exception{
		File dir = (this.getCheckPointDir(options));
		StringUtils.ensureDirExists(dir);
		

		dir = (this.getBackupDir(options));
		StringUtils.ensureDirExists(dir);

		dir = (this.getDataDir(options));
		StringUtils.ensureDirExists(dir);
		dir = this.getAttachmentDir();
		StringUtils.ensureDirExists(dir);

	}
	
	@Override
	public QueueStorage getOrCreateQueueStorage(String qname) throws Exception {
		FlumeQueueStorage  result = 
				queueStorages.putIfAbsent(qname, new FlumeQueueStorage(options, this.getQueueAttachmentDir(qname), qname, context));
		
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
		return new File(this.getAttachmentDir(), qname);
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
		prepareStorageBase(options);
		context = createContext(options);
	}
	@Override
	public void deleteQueueStorage(String name) {
		FlumeQueueStorage storage = queueStorages.remove(name);
		if(storage != null){
			storage.delete();
		}
		
	}
}
