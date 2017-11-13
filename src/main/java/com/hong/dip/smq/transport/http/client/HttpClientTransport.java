package com.hong.dip.smq.transport.http.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.camel.support.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.ChunkableDataSource;
import com.hong.dip.smq.ChunkableDataSource.ChunkReader;
import com.hong.dip.smq.Node;
import com.hong.dip.smq.RemoteQueue;
import com.hong.dip.smq.storage.MessageStorage;
import com.hong.dip.smq.storage.QueueStorage;
import com.hong.dip.smq.storage.flume.FlumeMessageStorage;
import com.hong.dip.smq.transport.ClientTransport;
import com.hong.dip.smq.transport.MessageCtrlException.FatalMessageException;
import com.hong.dip.smq.transport.MessageCtrlException.TemporaryMessageException;
import com.hong.dip.smq.transport.http.HttpConstants;
import com.hong.dip.smq.transport.http.HttpNetException;
import com.hong.dip.smq.transport.http.HttpStatusException;
import com.hong.dip.smq.transport.http.MsgTransportCmd;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgCheckCmd;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgCheckResult;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgPostCmd;
import com.hong.dip.smq.transport.http.client.HttpClientManager.HttpClient;

public class HttpClientTransport extends ServiceSupport implements ClientTransport {
	final static Logger log = LoggerFactory.getLogger(HttpClientTransport.class);
	HttpClientManager clientManager;
	Map<String, HttpMessageSender> senders = new ConcurrentHashMap<String, HttpMessageSender>();
	public HttpClientTransport(HttpClientOptions options){
		clientManager = new HttpClientManager(options);
	}
	@Override
	protected void doStart() throws Exception {
		clientManager.start();
	}

	@Override
	protected void doStop() throws Exception {
		for(String name : senders.keySet())
			this.stopMessageSender(name);
		
		for(String name : senders.keySet())
			senders.get(name).join(1000);
		
		clientManager.stop();
	}
	
	@Override
	public void startMessageSender( RemoteQueue remoteQueue, QueueStorage queue) {
		HttpMessageSender sender = new HttpMessageSender(remoteQueue.getDestNode(), queue, remoteQueue.getDestQueueName());
		
		if(senders.putIfAbsent(queue.getName(), sender) != null)
			return;
		sender.start();
		
	}
	@Override
	public void stopMessageSender(QueueStorage queue) {
		String name = queue.getName();
		stopMessageSender(name);
		
	}
	private void stopMessageSender(String name) {
		HttpMessageSender sender = senders.get(name);
		if(sender != null){
			sender.signalToExit();
		}
	}
	class HttpMessageSender extends Thread{

		private QueueStorage storage;
		private boolean exitFlag;
		private boolean mayDuplicated = true; //消息有可能重复发送
		private ByteBuffer buffer; //发送缓存
		HttpClient httpClient;
		public HttpMessageSender(Node node, QueueStorage storage, String destQueueName) {
			super("Http-Message-Sender " + storage.getName());
			this.storage = storage;
			httpClient = clientManager.createHttpClient(getURL(node, destQueueName));
			
		}

		private String getURL(Node node2, String destQueueName) {
			return new StringBuilder("http://")
					.append(node2.getIp()).append(":").append(node2.getPort())
					.append("/queue/").append(destQueueName).toString();
		}

		public void signalToExit() {
			exitFlag = true;
			this.interrupt();
		}
		
		public void run(){
			buffer = ByteBuffer.allocateDirect(storage.getChunkSize());
			while(!exitFlag){
				//try to take message from queue
				MessageStorage msg = null;
				try{
					msg = storage.take(3000);
				}catch(Exception e){
					log.error("queue("+ storage.getName()+") read message failed", e);
					rollbackOnException(e);
				
				}
				if(msg == null)
					continue;
				//take a message 
				try {
					if(mayDuplicated)
						checkAndSendMessage(msg);
					else
						sendMessage(msg, 0, 0);
					commitStorage();
					mayDuplicated = false;
				} catch (TemporaryMessageException e) {
					log.error("Temporary Error Occurs while sending  message. id ("+msg.getID()+") cause: " + e.getCause(), e);
					rollbackOnException(e.getCause());
					//sleep a while avoid loop-infinite is too busy
					try { Thread.sleep(3000); } catch (InterruptedException e1) {}
					mayDuplicated = true;
				} catch (FatalMessageException e) {
					// TODO: 以后需要添加到死信队列。目前暂时只记录日志
					log.error("Fatal Error Occurs while sending  message. id ("+msg.getID()+") cause: " + e.getCause(), e);
					commitStorage();
					mayDuplicated = false;
				}
			}
		}
		private void commitStorage() {
			try {
				storage.commit();
			} catch (Exception e) {
				log.error("queue("+ storage.getName()+") commit failed", e);
			}
			
		}
		private void rollbackOnException(Throwable e) {
			try {
				storage.rollback();
			} catch (Exception e1) {
				log.error("queue("+ storage.getName()+") rollback failed", e);
								
			}
			log.error("queue("+ storage.getName()+") rollback on exception", e);
		}
		public void checkAndSendMessage(MessageStorage msg) throws TemporaryMessageException, FatalMessageException{
			MsgCheckResult result = checkMessage(msg);
			
			if(result.getChunkIdx() == -1)//if 当前消息在接收端已经保存完整
				return; 
			
			this.sendMessage(msg, result.getPartIdx(), result.getChunkIdx());
			
		}

		private MsgCheckResult checkMessage(MessageStorage msg) throws FatalMessageException, TemporaryMessageException {
			MsgCheckCmd cmdCheck= new MsgCheckCmd(storage.getName(), 
					msg.getID(), 
					msg.getParts().size(), 
					storage.getChunkSize());
			return (MsgCheckResult)executeHttpCmd(msg, cmdCheck, null);
			
		}

		/**
		 * 从消息指定的Part和Chunk位置处开始发送消息内容。
		 * @param msg
		 * @param fromPartIndex
		 * @param fromChunkIndex
		 * @throws TemporaryMessageException
		 * @throws FatalMessageException
		 */
		public void sendMessage(MessageStorage msg, int fromPartIndex, long fromChunkIndex) 
				throws TemporaryMessageException, FatalMessageException{
			List<ChunkableDataSource> allParts = msg.getParts();
			boolean firstPart = true;
			for(int partIdx = fromPartIndex; partIdx < allParts.size(); partIdx++){
				long chunkIdx = firstPart ? fromChunkIndex : 0;
				if(chunkIdx >= 0){
					ChunkableDataSource ds = allParts.get(partIdx);
					ChunkReader reader = null;
					
					try{
						reader = ds.openReader();
						reader.pos(chunkIdx, this.storage.getChunkSize());
						
						ByteBuffer buffer;
						while((buffer = reader.readChunk(this.buffer)) != null){
							long chunkOffset = reader.getReadedChunkOffset();
							MsgPostCmd postCmd = createMsgPostCmd(msg, allParts, partIdx, reader, chunkOffset, buffer);
							executeHttpCmd(msg, postCmd, buffer);/*if success always return a confirm cmd, we doesnt' care its content*/
						}
					
					}catch(IOException e){
						String desc = "!!!! Fatal error, cannot read chunk from queue ("+this.storage.getName()+") message ("+msg.getID()+")";
						log.error(desc, e);
						throw new FatalMessageException(desc, e);
					
					}finally{
						if(reader != null) reader.close();
					}
				}//else chunkIndex < 0 means this part's is already completed;
				firstPart = false;
			}
		}

		private MsgTransportCmd executeHttpCmd(MessageStorage msg, MsgTransportCmd httpCmd, ByteBuffer buffer)
				throws FatalMessageException, TemporaryMessageException {
			try {
				return this.httpClient.executeCmd(httpCmd, buffer);
			} catch (HttpStatusException e) {
				if(e.getStatusCode() == HttpConstants.STATUS_CONFLICT){
					FatalMessageException e1 = new FatalMessageException(
							"Fatal Exception occurs. queue("+storage.getName()+") messaage ("+msg.getID()+")",
							e);
					log.error(e1.getMessage(), e);
					throw e1;
				}else{
					throw logAndCreateTemporaryException(msg, e);
				}
			} catch (HttpNetException e) {
				throw logAndCreateTemporaryException(msg, e);
			}
		}

		private MsgPostCmd createMsgPostCmd(MessageStorage msg, List<ChunkableDataSource> allParts, int partIdx,
				ChunkReader reader, long chunkOffset, ByteBuffer buffer) throws IOException {
			MsgPostCmd postCmd = new MsgPostCmd(this.storage.getName(), msg.getID(), allParts.size(), partIdx, reader.getSize(), chunkOffset, buffer.remaining());
			postCmd.setAttachmentNames(((FlumeMessageStorage)msg).getAttachmentNames());
			return postCmd;
		}
		private TemporaryMessageException logAndCreateTemporaryException(MessageStorage msg, Exception cause){
			TemporaryMessageException e1 =new TemporaryMessageException(
				"Temporary exception occurs. queue("+storage.getName()+") messaage ("+msg.getID()+")",
				cause);
			log.error(e1.getMessage(), cause);
			return e1;
		}
	}
	

}
