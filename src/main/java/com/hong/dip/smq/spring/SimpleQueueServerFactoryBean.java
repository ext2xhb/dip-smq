package com.hong.dip.smq.spring;

import org.springframework.beans.factory.FactoryBean;

import com.hong.dip.smq.QueueServer;
import com.hong.dip.smq.server.SimpleQueueServer;
import com.hong.dip.smq.server.SimpleQueueServer.SimpleQueueServerFactory;
import com.hong.dip.smq.storage.flume.FlumeOptions;
import com.hong.dip.smq.transport.http.client.HttpClientOptions;
import com.hong.dip.smq.transport.http.server.JettyOptions;

public class SimpleQueueServerFactoryBean implements FactoryBean {
	FlumeOptions storageOptions;
	HttpClientOptions clientOptions;
	JettyOptions serverOptions;

	
	SimpleQueueServer  server;
	@Override
	public Object getObject() throws Exception {
		if(server == null){
			server = new SimpleQueueServerFactory()
				.createSimpleQueueServer(storageOptions, clientOptions, serverOptions);
			server.start();
		}
		return server;
	}

	@Override
	public Class getObjectType() {
		return QueueServer.class;
	}

	@Override
	public boolean isSingleton() {
		return true;
	}
	
	public FlumeOptions getStorageOptions() {
		return storageOptions;
	}

	public void setStorageOptions(FlumeOptions storageOptions) {
		this.storageOptions = storageOptions;
	}

	public HttpClientOptions getClientOptions() {
		return clientOptions;
	}

	public void setClientOptions(HttpClientOptions clientOptions) {
		this.clientOptions = clientOptions;
	}

	public JettyOptions getServerOptions() {
		return serverOptions;
	}

	public void setServerOptions(JettyOptions serverOptions) {
		this.serverOptions = serverOptions;
	}

	
}
