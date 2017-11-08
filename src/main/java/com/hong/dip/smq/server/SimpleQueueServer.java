package com.hong.dip.smq.server;

import org.apache.camel.support.ServiceSupport;

import com.hong.dip.smq.Node;
import com.hong.dip.smq.Queue;
import com.hong.dip.smq.QueueServer;
import com.hong.dip.smq.storage.flume.FlumeOptions;
import com.hong.dip.smq.storage.flume.FlumeStorage;
import com.hong.dip.smq.transport.http.HttpClientManager;
import com.hong.dip.smq.transport.http.HttpClientOptions;
import com.hong.dip.smq.transport.http.JettyOptions;
import com.hong.dip.smq.transport.http.JettyServer;

public class SimpleQueueServer extends ServiceSupport implements QueueServer{

	
	private FlumeOptions storageOptions;
	private HttpClientOptions clientOptions;
	private JettyOptions serverOptions;
	private FlumeStorage flumeStorage;
	private HttpClientManager httpClientManager;
	private JettyServer jettyServer;
	


	public SimpleQueueServer(FlumeOptions storageOptions, HttpClientOptions clientOptions, JettyOptions serverOptions) {
		this.storageOptions = storageOptions;
		this.clientOptions = clientOptions;
		this.serverOptions = serverOptions;
	}

	public void doStart() throws Exception{
		this.flumeStorage = new FlumeStorage(this.storageOptions);
		this.httpClientManager = new HttpClientManager(this.clientOptions);
		if(serverOptions != null){
			this.jettyServer = new JettyServer(this.serverOptions);
		}
		flumeStorage.start();
		httpClientManager.start();
		jettyServer.start();
		
		
	}
	public void doStop() throws Exception{
		
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Queue createRemoteQueue(String qname, Node node) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
}
