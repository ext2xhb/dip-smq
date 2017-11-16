package com.hong.dip.smq.server;

import org.apache.camel.support.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.MessagePostHandler;
import com.hong.dip.smq.Node;
import com.hong.dip.smq.Queue;
import com.hong.dip.smq.QueueServer;
import com.hong.dip.smq.RemoteQueue;
import com.hong.dip.smq.storage.QueueStorage;
import com.hong.dip.smq.storage.Storage;
import com.hong.dip.smq.storage.flume.FlumeOptions;
import com.hong.dip.smq.storage.flume.FlumeStorage;
import com.hong.dip.smq.transport.ClientTransport;
import com.hong.dip.smq.transport.ServerTransport;
import com.hong.dip.smq.transport.http.client.HttpClientOptions;
import com.hong.dip.smq.transport.http.client.HttpClientTransport;
import com.hong.dip.smq.transport.http.server.JettyOptions;
import com.hong.dip.smq.transport.http.server.JettyTransport;

public class SimpleQueueServer extends ServiceSupport implements QueueServer{
	static final Logger log = LoggerFactory.getLogger(SimpleQueueServer.class);
	
	
	private Storage storage;
	private ClientTransport clientTransport	;
	private ServerTransport servertransport;
	
	private String nodeName; //当前服务器的名字

	public SimpleQueueServer(FlumeOptions storageOptions, HttpClientOptions clientOptions, JettyOptions serverOptions) {
		this.storage = new FlumeStorage(storageOptions);
		this.clientTransport = new HttpClientTransport(clientOptions);
		if(serverOptions != null){
			this.servertransport = new JettyTransport(serverOptions);
		}
		this.nodeName = clientOptions.getNodeName();
	}

	public void doStart() throws Exception{
		storage.start();
		clientTransport.start();
		if(servertransport != null)
			servertransport.start();
	}
	public void doStop() throws Exception{
		if(servertransport != null)
			this.servertransport.stop();
		this.clientTransport.stop();
		
		this.storage.stop();
	}
	public static class SimpleQueueServerFactory{
		/**
		 * @param storageOptions: queue storage configurations, must specified
		 * @param clientOptions:  queue-sender configurations, if we want to send message to queue , must be provided;
		 * @param serverOptions:  queue-receiver configurations, if want to receive message from remote client , must be provided;
		 * @return
		 */
		public SimpleQueueServer  createSimpleQueueServer(FlumeOptions storageOptions, HttpClientOptions clientOptions, JettyOptions serverOptions){
			return  new SimpleQueueServer(storageOptions, clientOptions, serverOptions);
			
		}
	}
	@Override
	public Queue createQueue(String qname) throws Exception {
		QueueStorage queueStorage = this.storage.getOrCreateQueueStorage(qname);
		if(this.servertransport != null){
			servertransport.startMessageReceiver(queueStorage);
		}
		return queueStorage.getQueue();
		
	}

	@Override
	public RemoteQueue createRemoteQueue(String qname, Node node, MessagePostHandler handler) throws Exception {
		try{
		//发送队列名是本地节点名+队列名
		QueueStorage queue = this.storage.getOrCreateQueueStorage(
				nodeName + "_" + qname);
		queue.setMessagePostHandler(handler);
		RemoteQueue rQueue = new RemoteQueue(node, qname, queue);
		clientTransport.startMessageSender(rQueue, queue);
		return rQueue;
		}catch(Exception e){
			log.error("Cannot create remote queue("+qname + " " +node+")", e);
			throw e;
		}
	}

	@Override
	public void deleteQueue(Queue queue) throws Exception {
		this.storage.deleteQueueStorage(queue.getName());
		if(servertransport != null){
			servertransport.stopMessageReceiver(queue.getStorage());
		}
		clientTransport.stopMessageSender(queue.getStorage());
		
		
	}
}
