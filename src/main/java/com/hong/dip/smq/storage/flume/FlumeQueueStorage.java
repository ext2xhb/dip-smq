package com.hong.dip.smq.storage.flume;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.support.ServiceSupport;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.conf.Configurables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.Message;
import com.hong.dip.smq.Queue;
import com.hong.dip.smq.SimpleMessage;
import com.hong.dip.smq.storage.MessageStorage;
import com.hong.dip.smq.storage.MessageWriter;
import com.hong.dip.smq.storage.QueueStorage;
import com.hong.dip.utils.StringUtils;

public class FlumeQueueStorage extends ServiceSupport implements Queue, QueueStorage {
	static final Logger log = LoggerFactory.getLogger(FlumeQueueStorage.class);
	private static final String FILE_CHECK_LOG = "___check_log___";
	private FileChannel channel;
	private ThreadLocal<Transaction> flumeTransaction = new ThreadLocal<Transaction>();
	
	private String qname;
	private Context context;
	private File attachmentDir;
	private AtomicLong messageSequnce = new AtomicLong();
	private Semaphore msgSemaphore = new Semaphore(0); //只是优化take的等待时间，不能严格依赖该信号进行同步控制
	private FlumeOptions options;
	private Map<String, MessageWriter> messageWriters = new ConcurrentHashMap<String, MessageWriter>();
	private FlumeMessageCheckLog msgCheckLog;
	
	public FlumeQueueStorage(FlumeOptions options, File attachmentDir, String qname, Context context){
		this.options = options;
		this.qname = qname;
		this.context = context;
		this.attachmentDir = attachmentDir;
		msgCheckLog = new FlumeMessageCheckLog(new File(attachmentDir, FILE_CHECK_LOG));
	}
	public FlumeMessageCheckLog getMsgCheckLog(){
		return this.msgCheckLog;
	}
	public String getName(){
		return channel.getName();
	}
	public FileChannel getChannel(){
		return channel;
	}
	@Override
	protected void doStart() throws Exception {
		StringUtils.ensureDirExists(attachmentDir);
		msgCheckLog.open();
		FileChannel channel = new FileChannel();
		channel.setName(qname);
		Configurables.configure(channel, context);
		this.channel = channel;
		this.channel.start();
		flumeTransaction.set(null);;
	}
	@Override
	protected void doStop() throws Exception {
		this.rollback();
		this.channel.stop();
		msgCheckLog.close();
		
	}
	/**
	 * delete channel
	 */
	public void delete() {
		// TODO !!!!!!!!!delete queue;
	}

	@Override
	public boolean putMessage(Message m) throws Exception {
		//ensure message id exits;
		if(m.getID() == null)
			m.setID(this.newMessageID());
		
		MessageStorage storage = this.messageToStorage(m);
		return this.offer(storage);
	}
	@Override
	public Message getMessage(int millis) throws Exception {
		MessageStorage storage = this.take(millis);
		if(storage != null)
			return this.storageToMessage(storage);
		else
			return null;
	}
	
	public boolean offer(MessageStorage msgStorage) throws Exception{
		ensureTransaction();
		try{
			//set a sequence , stored in queue storage
			//msgStorage.setStoreSequence(this.newMessageSequence());
			this.channel.put(msgStorage.getBodyContent(Event.class));
			
			this.msgSemaphore.release();
			return true;
		}catch(ChannelFullException e){
			log.error("queue " + this.qname +"is full", e);
			return false;
		}
	}
	public MessageStorage take(int millis) throws Exception
	{
		ensureTransaction();
		MessageStorage msg = null;
		//尝试读取消息，有的话释放信号量
		Event event = takeEvent();
		if(event != null){
			msgSemaphore.tryAcquire();
			msg = new FlumeMessageStorage(event);
		}
		//if no message, try to waiting for new message in given time;
		if(msg == null){
			long expired = System.currentTimeMillis() + (long)millis;
			long currTime;
			while((currTime = System.currentTimeMillis()) < expired){
				long wait = expired - currTime;
				if(wait > 1000) 
					wait = 1000;
				msgSemaphore.tryAcquire(wait, TimeUnit.MILLISECONDS);
				try{
					event = takeEvent();
				}catch(Exception e){
					msgSemaphore.release();
				}
				if(event != null){
					msg = new FlumeMessageStorage(event);
					break;
				}
			}
		}
		return msg;
		
	}
	
	private Event takeEvent() throws Exception {
		Event event = null;
		try{
			event = this.channel.take();
		}catch(Exception e){
			log.error("failed to take message from queue(" + this.getName()+")", e);
			//TODO : 可能导致消息处理停止（无法读取后续消息）: 后续需要考虑如何严谨处理此错误
			this.rollback();
			throw e;
		}
		return event;
	}


	private void ensureTransaction() {
		if(this.flumeTransaction.get() != null)
			return;
		else{
			this.flumeTransaction.set(channel.getTransaction());
			this.flumeTransaction.get().begin();
		}
		
	}	@Override
	public void commit() throws Exception {
		try{
			if(this.flumeTransaction.get() != null){
				this.flumeTransaction.get().commit();
				this.flumeTransaction.get().close();
				this.flumeTransaction.set(null);
			}
		}catch(Exception e){
			log.error("Failed to commit on Queue("+this.getName()+")", e);
			throw e;
		}
		
	}
	@Override
	public void rollback() throws Exception {
		try{
			if(this.flumeTransaction.get() != null){
				this.flumeTransaction.get().rollback();
				this.flumeTransaction.get().close();
				this.flumeTransaction.set(null);
			}
		}catch(Exception e){
			log.error("Failed to rollback on Queue("+this.getName()+")", e);
			throw e;
		}
	}
	
	@Override
	public Message createMessage() {
		Message msg = new SimpleMessage();
		msg.setID(this.newMessageID());
		return msg;
	}
	@Override
	public String newMessageID() {
		return new StringBuilder(UUID.randomUUID().toString())
				.append('-')
				.append(Long.toString(newMessageSequence())).toString();
	}
	@Override
	public long newMessageSequence() {
		return this.messageSequnce.incrementAndGet();
	}

//	@Override
//	public ChunkableDataSource getAttachmentStore(MessageStorage message, int idx, String name)/* throws Exception*/ {
//		File file = getAttachmentFileName(message.getID(), idx, name);
//		/*//doesn't check if attachment file exists (避免在用户不知道的情况下丢失消息)
//		if(!file.isFile()){
//			throw new IOException("Attachment file (" + file.toString() + ") not exits");
//		}
//		*/
//		return new ChunkableFileDataSource(name, file);
//	}
	
//	private File getAttachmentFileName(String id, int idx, String name) {
//		return new File(this.attachmentDir, 
//				new StringBuilder(id).append("-").append(idx).append("-").append(name).toString());
//		
//	}
	@Override
	public Queue getQueue() {
		return this;
	}
	@Override
	public QueueStorage getStorage() {
		return this;
	}
	@Override
	public MessageStorage messageToStorage(Message message) {
		return FlumeMessageStorage.cast2Storage(message);
	}
	@Override
	public Message storageToMessage(MessageStorage messageStorage) {
		return FlumeMessageStorage.cast2Message( messageStorage);
	}
	@Override
	public int getChunkSize() {
		return options.getChunkSize();
	}
	@Override
	public MessageWriter getOrOpenMessageWriter(String remoteQueue, String msgId) throws IOException {
		if(messageWriters.putIfAbsent(remoteQueue, new FlumeMessageWriter(this, this.attachmentDir, remoteQueue)) == null){
			messageWriters.get(remoteQueue).open();
		}
		return messageWriters.get(remoteQueue);
	}
	@Override
	public MessageWriter getCurrentMessageWriter(String remoteQueue) {
	
		return this.messageWriters.get(remoteQueue);
	}
	public FlumeMessageCheckLog getFlumeMessageCheckLog() {
		return this.msgCheckLog;
	}


}
