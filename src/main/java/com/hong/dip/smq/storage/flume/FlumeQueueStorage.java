package com.hong.dip.smq.storage.flume;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.support.ServiceSupport;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.ChunkableDataSource;
import com.hong.dip.smq.ChunkableFileDataSource;
import com.hong.dip.smq.Message;
import com.hong.dip.smq.Queue;
import com.hong.dip.smq.storage.MessageStorage;
import com.hong.dip.smq.storage.QueueStorage;
import com.hong.dip.utils.StringUtils;

public class FlumeQueueStorage extends ServiceSupport implements Queue, QueueStorage {
	static final Logger log = LoggerFactory.getLogger(FlumeQueueStorage.class);
	private static final String HEADER_ID = "id";
	private static final String HEADER_NAMELIST = "ats";
	private FileChannel channel;
	private Transaction flumeTransaction = null;
	
	private String qname;
	private Context context;
	private File attachmentDir;
	private AtomicLong messageSequnce = new AtomicLong();
	
	public FlumeQueueStorage(File attachmentDir, String qname, Context context){
		this.qname = qname;
		this.context = context;
		this.attachmentDir = attachmentDir;
	}
	public String getName(){
		return channel.getName();
	}
	public FileChannel getChannel(){
		return channel;
	}
	@Override
	protected void doStart() throws Exception {
		FileChannel channel = new FileChannel();
		channel.setName(qname);
		Configurables.configure(channel, context);
		this.channel = channel;
		this.channel.start();
		flumeTransaction = null;
		StringUtils.ensureDirExists(attachmentDir);
	}
	@Override
	protected void doStop() throws Exception {
		if(this.flumeTransaction != null){
			this.flumeTransaction.rollback();
			this.flumeTransaction = null;
		}
		this.channel.stop();
		
	}
	@Override
	public boolean putMessage(Message m) throws Exception {
		ensureTransaction();
		try{
			this.channel.put(makeEvent(m));
			return true;
		}catch(ChannelFullException e){
			log.error("queue " + this.qname +"is full", e);
			return false;
		}
	}

	@Override
	public Message getMessage() throws Exception {
		ensureTransaction();
		Event event = null;
		try{
			event = this.channel.take();
		}catch(Exception e){
			log.error("failed to take message from queue(" + this.getName()+")", e);
			//TODO : 可能导致消息处理停止（无法读取后续消息）: 后续需要考虑如何严谨处理此错误
			this.rollback();
			throw e;
		}
		if(event == null)
			return null;
		//附件文件不完整也返回消息（应用需要处理附件无法打开的情况）
		//try{
			return makeMessage(event);
		//}catch(Exception e){
		//	log.error("!!!!!!!!Taken Event cannot convert to message maybe lost data(" + this.getName()+")", e);
		//	this.commit();
			//TODO: 可能导致数据丢失，后续需要考虑严谨处理此错误（建议将此消息放入死信队列中）
		//	throw e;
		//}
		
	}
	private Message makeMessage(Event event) {
		FlumeMessageStorage message = this.createMessageStorage();
		message.updateID(event.getHeaders().get(HEADER_ID));
		message.setByteBody(event.getBody());
		
		//deal with attachments
		String names = event.getHeaders().get(HEADER_NAMELIST);
		if(names != null){
			List<String> attachmentNames = StringUtils.string2List(names);
			for(int idx = 0; idx < attachmentNames.size(); idx++){
				String name = attachmentNames.get(idx);
				ChunkableDataSource ds = ((QueueStorage)this).getAttachmentStore((MessageStorage)message, idx, name);
				message._addAttachment(ds);
			}
			
		}
		return message;
	}
	private Event makeEvent(Message m) {
		SimpleEvent event = new SimpleEvent();
		String id = m.getID();
		event.getHeaders().put(HEADER_ID, id);
		event.getHeaders().put(HEADER_NAMELIST, StringUtils.list2String(m.getAttachmentNameList()));
		event.setBody(m.getByteBody());
		return event;
		
	}
	private void ensureTransaction() {
		if(this.flumeTransaction != null)
			return;
		else
			this.flumeTransaction = channel.getTransaction();
		
	}	@Override
	public void commit() throws Exception {
		if(this.flumeTransaction != null){
			this.flumeTransaction.commit();
			this.flumeTransaction = null;
		}
		
	}
	@Override
	public void rollback() throws Exception {
		if(this.flumeTransaction != null){
			this.flumeTransaction.rollback();;
			this.flumeTransaction = null;
		}
	}
	@Override
	public Message createMessage() {
		FlumeMessageStorage msg = new FlumeMessageStorage();
		msg.updateID(((QueueStorage)this).allocateMessageID());
		return null;
	}
	private FlumeMessageStorage createMessageStorage() {
		return new FlumeMessageStorage();
	}
	@Override
	public String allocateMessageID() {
		return new StringBuilder(UUID.randomUUID().toString())
				.append("-")
				.append(this.newMessageSequence()).toString();
	}
	private long newMessageSequence() {
		return this.messageSequnce.incrementAndGet();
	}
	@Override
	public ChunkableDataSource getAttachmentStore(MessageStorage message, int idx, String name)/* throws Exception*/ {
		File file = getAttachmentFileName(message.getID(), idx, name);
		/*//doesn't check if attachment file exists (避免在用户不知道的情况下丢失消息)
		if(!file.isFile()){
			throw new IOException("Attachment file (" + file.toString() + ") not exits");
		}
		*/
		return new ChunkableFileDataSource(name, file);
	}
	private File getAttachmentFileName(String id, int idx, String name) {
		return new File(this.attachmentDir, 
				new StringBuilder(id).append("-").append(idx).append("-").append(name).toString());
		
	}


}
