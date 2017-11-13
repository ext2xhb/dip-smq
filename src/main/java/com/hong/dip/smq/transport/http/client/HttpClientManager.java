package com.hong.dip.smq.transport.http.client;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.apache.camel.support.ServiceSupport;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.config.SocketConfig;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.transport.http.HttpConstants;
import com.hong.dip.smq.transport.http.HttpInvalidCmdException;
import com.hong.dip.smq.transport.http.HttpNetException;
import com.hong.dip.smq.transport.http.HttpStatusException;
import com.hong.dip.smq.transport.http.MsgTransportCmd;
import com.hong.dip.smq.transport.http.MsgTransportCmd.HeaderReader;
import com.hong.dip.smq.transport.http.MsgTransportCmd.HeaderWriter;
import com.hong.dip.utils.StringUtils;

public class HttpClientManager extends ServiceSupport{   
	static final Logger log = LoggerFactory.getLogger(HttpClientManager.class);
	
	public static final byte[] zeroBytes = new byte[0];
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

	@Override
	protected void doStart() throws Exception {
	}

	@Override
	protected void doStop() throws Exception {
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
			//url must end with '/'
			if(url.charAt(url.length() - 1) != '/')
				url = url + "/";
			this.url = url;
		}
		
		public MsgTransportCmd postMethod(MsgTransportCmd cmd, InputStream input, int contentLength) throws HttpStatusException, HttpNetException{
			//set http post;
			HttpPost post = new HttpPost(this.url);
			
			setCmd2Post(cmd, post);
			setPostInput(input, contentLength, post);
			
			//execute post
			CloseableHttpResponse response = null;
			try {
				response= backClient.execute(post);
				
				int statusCode = getStatusCode(response);
				
				MsgTransportCmd result = null;
				if(statusCode == HttpConstants.STATUS_OK){
					result = parseCmd(response);
					if(result == null)
						new MsgTransportCmd.MsgSuccessConfirm();
					return result;
				}else{
					throw new HttpStatusException(statusCode, 
							responseAsString(response));
				}
				
			} catch (ClientProtocolException e) {
				throw new HttpNetException(e.getMessage(), e);
			} catch (IOException e) {
				throw new HttpNetException(e.getMessage(), e);
			}finally{
				if(response != null)
					releaseResponse(response);
			}
		}

		private String responseAsString(CloseableHttpResponse response) throws HttpNetException{
			InputStream is = null;
			try{
				if(response.getEntity() != null)
				is = response.getEntity().getContent();
				if(is != null){
					byte[] content = StringUtils.readInputStreamContent(is);
					return new String(content, "UTF-8");
				}else
					return "";
			}catch(IOException e){
				throw new HttpNetException(e.toString(), e);
			}finally{
				if(is != null)
					try {	is.close();	} catch (IOException e) {}
			}
		}

		private int getStatusCode(CloseableHttpResponse response) throws IOException{
			StatusLine status = response.getStatusLine();
			if(status == null)
				throw new IOException ("reponse status unknown");
			return status.getStatusCode();
		}

		private void setPostInput(InputStream input, int contentLength, HttpPost post) {
			if(input == null)
				input = new ByteArrayInputStream(zeroBytes);
			
			HttpEntity entity = new InputStreamEntity(input, contentLength);
			post.setEntity(entity);
			
			
		}

		private void setCmd2Post(MsgTransportCmd cmd, final HttpPost post) {
			cmd.writeCmd(new HeaderWriter(){
				@Override
				public void putHeader(String header, String value) {
					post.setHeader(header, value);
				}
			});
		}
		

		private void releaseResponse(CloseableHttpResponse response) {
			HttpEntity entity = response.getEntity();
			if(entity != null){
				try {
					consumeAndClose(entity.getContent());
				} catch (IOException e) {
				}
			}
			try {
				response.close();
			} catch (IOException e) {
				log.error("close reponse failed", e);
			}
		}

		private MsgTransportCmd parseCmd(final CloseableHttpResponse r) throws HttpInvalidCmdException {
			return MsgTransportCmd.parseCmd(new HeaderReader(){

				@Override
				public String getHeader(String name) {
					Header h = r.getFirstHeader(name);
					if(h == null)
						return null;
					return h.getValue();
				}
				
			});
		}

		private void consumeAndClose(InputStream content) {
			if(content == null)
				return;
			try{
				try{
					while(content.read() != -1){
						;//do nothing
					}
				}finally{
					content.close();
				}
			}catch(IOException e){
				log.error("consumeAndClose failed", e);
			}
		}

		public MsgTransportCmd executeCmd(MsgTransportCmd cmd, ByteBuffer buffer) throws HttpStatusException, HttpNetException {
			return this.postMethod(cmd, 
				buffer == null ? null :new StringUtils.ByteBufferInputStream(buffer), 
				buffer == null ? 0 : buffer.remaining());
			
		}
	}
	


}  