package com.hong.dip.smq.transport.http;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.camel.support.ServiceSupport;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

public class HttpClientManager extends ServiceSupport{   
	
	private HttpClientOptions options;
	PoolingHttpClientConnectionManager connManager;

	public HttpClientManager(HttpClientOptions options){
		this.options = options;
        // Create a connection manager with custom configuration.
        this.connManager = new PoolingHttpClientConnectionManager(options.getConnLivingTime(), TimeUnit.MILLISECONDS);
        // setup socket configuration
        SocketConfig socketConfig = SocketConfig.custom()
            .setTcpNoDelay(true)
            .setSoTimeout(options.getSoTimeOut())
            .setSoKeepAlive(options.isSoKeepAlive())
            .build();
        connManager.setDefaultSocketConfig(socketConfig);
        connManager.setValidateAfterInactivity(options.getConnCheckInterval());

        // setup connection configuration
        //ConnectionConfig connectionConfig = ConnectionConfig.custom()
        //     .setMalformedInputAction(CodingErrorAction.IGNORE)
        //    .setUnmappableInputAction(CodingErrorAction.IGNORE)
        //    .setCharset(Consts.UTF_8)
        //    .build();
        //connManager.setDefaultConnectionConfig(connectionConfig);
        
        connManager.setMaxTotal(options.getMaxConnection());
        connManager.setDefaultMaxPerRoute(options.getMaxConnPerClient());
        
	}
	
	private RequestConfig createDefaultRequestConfig(){
		return RequestConfig.custom()
        		.setSocketTimeout(options.getSocketTimeout())
        		.setConnectTimeout(options.getConnectionTimeout())
        		.setConnectionRequestTimeout(options.getConnectionRequestTimeout())
        		.build();
        
	}
	public HttpClient createHttpClient(String url){
		CloseableHttpClient backClient  = HttpClients.custom()
			.setConnectionManager(this.connManager)
			.setDefaultRequestConfig(this.createDefaultRequestConfig())
			.build();
		return new HttpClient(backClient, url);
	}
	public static class HttpClient{
		
		private CloseableHttpClient backClient;
		private String url;

		public HttpClient(CloseableHttpClient backClient, String url){
			this.backClient = backClient;
			this.url = url;
		}
		public void getMethod(Map<String, String> params, HttpResponseHandler handler) throws HttpStatusException, HttpNetException, InvocationTargetException{
			
		}
		public void postMethod(InputStream input, HttpResponseHandler handler) throws HttpStatusException, HttpNetException, InvocationTargetException{
			
		}
	}
	public static interface HttpResponseHandler{
		void handle(int httpStatus, InputStream is, HttpEntity response) throws Exception;
	}
	@Override
	protected void doStart() throws Exception {
	}

	@Override
	protected void doStop() throws Exception {
		// TODO Auto-generated method stub
		
	}
	


}  