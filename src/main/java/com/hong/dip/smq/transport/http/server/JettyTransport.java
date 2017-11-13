package com.hong.dip.smq.transport.http.server;

import java.io.IOException;
import java.io.InputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.storage.MessageWriter;
import com.hong.dip.smq.storage.MessageWriter.MessagePosition;
import com.hong.dip.smq.storage.QueueStorage;
import com.hong.dip.smq.transport.MessageCtrlException.FatalMessageException;
import com.hong.dip.smq.transport.MessageCtrlException.TemporaryMessageException;
import com.hong.dip.smq.transport.ServerTransport;
import com.hong.dip.smq.transport.http.HttpConstants;
import com.hong.dip.smq.transport.http.HttpInvalidCmdException;
import com.hong.dip.smq.transport.http.MsgTransportCmd;
import com.hong.dip.smq.transport.http.MsgTransportCmd.HeaderWriter;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgCheckCmd;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgCheckResult;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgPostCmd;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgSuccessConfirm;
import com.hong.dip.utils.StringUtils;

public class JettyTransport implements ServerTransport {
	
	final static Logger log = LoggerFactory.getLogger(JettyTransport.class);
	private JettyServer server;

	public JettyTransport(JettyOptions options){
		this.server = new JettyServer(options);
	}
	@Override
	public void start() throws Exception {
		server.start();
	}

	@Override
	public void stop() throws Exception {
		server.stop();
	}

	@Override
	public void startMessageReceiver(QueueStorage queue) throws Exception {
		try{
			server.addServant(
					"/queue/" + queue.getName(), new JettyHandler(queue));
		}catch(Exception e){
			log.error("cannot add url handler for queue " + queue.getName(), e);
			throw e;
		}
	}
	@Override
	public void stopMessageReceiver(QueueStorage queue) {
		try{
			server.removeServant("/queue/" + queue.getName());
		}catch(Exception e){
			log.error("cannot unregister url handler for queue " + queue.getName(), e);

		}
	}

	class JettyHandler extends AbstractHandler{
		private QueueStorage storage;
		public JettyHandler(QueueStorage queue){
			this.storage = queue;
		}
		@Override
		public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response){
			try{
				MsgTransportCmd cmd;
				try {
					cmd = MsgTransportCmd.parseCmd(request, storage);
				} catch (HttpInvalidCmdException e) {
					String err = "Invalide command ; queue("+storage.getName()+") request("+request.getContextPath()+")";
					log.error(err, e);
					sendErrorResponse(response, HttpConstants.STATUS_UNAVAILABLE,err);
					return;
					
				}
				switch(cmd.getCmdType()){
				case MSG_POST_CHUNK:
					MsgTransportCmd.MsgSuccessConfirm confirm;
					try {
						confirm = executeMsgPost((MsgPostCmd)cmd, storage, request);
						sendMsgTransportCmd(response, confirm);
					} catch (TemporaryMessageException e) {
						log.error("Temporary Error Occurs; cause " + e.getCause(), e);
						sendErrorResponse(response, HttpConstants.STATUS_UNAVAILABLE, 
							new StringBuilder(e.getMessage()).append(" cause:").append(e.getCause().getMessage()).toString());
						return;
						
					} catch (FatalMessageException e) {
						log.error("Fatal Error Occurs; cause " + e.getCause(), e);
						sendErrorResponse(response, HttpConstants.STATUS_CONFLICT, 
								new StringBuilder(e.getMessage()).append(" cause:").append(e.getCause().getMessage()).toString());
							return;
					}
					
					break;
				case MSG_CHECK_MSG:
					try {
						MsgTransportCmd.MsgCheckResult result = executeMsgCheck((MsgCheckCmd)cmd, storage);
						sendMsgTransportCmd(response, result);
					} catch (TemporaryMessageException e) {
						log.error("Temporary Error Occurs; cause " + e.getCause(), e);
						sendErrorResponse(response, HttpConstants.STATUS_UNAVAILABLE, 
							new StringBuilder(e.getMessage()).append(" cause:").append(e.getCause().getMessage()).toString());
						return;
						
					} catch (FatalMessageException e) {
						log.error("Fatal Error Occurs; cause " + e.getCause(), e);
						sendErrorResponse(response, HttpConstants.STATUS_CONFLICT, 
								new StringBuilder(e.getMessage()).append(" cause:").append(e.getCause().getMessage()).toString());
							return;
					}
					break;
				default:
					String err = "Unkown transport cmd: " + request.toString();
					log.error(err);
					sendErrorResponse(response, HttpConstants.STATUS_UNAVAILABLE, err);
					break;
				}
				
			}finally{
				baseRequest.setHandled(true);
			}
		}
		private void sendMsgTransportCmd(HttpServletResponse response, MsgTransportCmd cmd){
			response.setStatus(HttpConstants.STATUS_OK);
			response.setContentLength(0);
			cmd.writeCmd(new HeaderWriter(){
				@Override
				public void putHeader(String name, String value) {
					response.setHeader(name, value);
				}
				
			});
			
		}
		private MsgCheckResult executeMsgCheck(MsgCheckCmd cmd, QueueStorage queue) 
				throws TemporaryMessageException, FatalMessageException{
			//queue.checkMessage(cmd.getSenderQueueName(), cmd.getMsgId(), cmd.getPartNum(), cmd.getChunkSize());
			MessageWriter writer;// = queue.getCurrentMessageWriter(cmd.getSenderQueueName());
			try{
				writer = queue.getOrOpenMessageWriter(cmd.getSenderQueueName(), cmd.getMsgId());
				MessagePosition position = writer.checkPositionToWrite(cmd.getMsgId(), cmd.getPartNum());
				return new MsgCheckResult(position.getPartIndex(), position.getChunkIndex());
			}catch(IOException e){
				TemporaryMessageException e1 = new TemporaryMessageException("Temporary Error Occurs; save message to queue("+queue.getName()+")", e); //TODO: ??现在简单处理，让客户端重试；正确做法应该是让客户端死信；
				log.error(e1.getMessage(), e);
				MessageWriter.MessagePosition pos = new MessageWriter.MessagePosition(0, 0); //从最开始位置重新发送消息
				return new MsgCheckResult(pos.getPartIndex(), pos.getChunkIndex());
			}
		}
			
		private MsgSuccessConfirm executeMsgPost(MsgPostCmd cmd, QueueStorage queue, HttpServletRequest request) 
				throws TemporaryMessageException, FatalMessageException{
			MessageWriter writer;
			try {
				writer = queue.getOrOpenMessageWriter(cmd.getSenderQueue(), 
						cmd.getMsgId());
			} catch (IOException e) {
				TemporaryMessageException e1 = new TemporaryMessageException("Temporary Error Occurs; cannot open MessageWriter; ("+queue.getName()+")", e); //TODO: ??现在简单处理，让客户端重试；正确做法应该是让客户端死信；
				log.error(e1.getMessage(), e);
				throw e1;
			}
			try{
				if(!writer.isWritingMsg(cmd.getMsgId())){
					writer.startNewMessage(cmd.getMsgId(), 
							StringUtils.string2List(cmd.getAttachmentNames()), cmd.getPartNum());
				}
				writer.writeChunk(cmd.getPartIndex(), 
						cmd.getPartLength(), cmd.getChunkOffset(), cmd.getChunkLen(), (InputStream)request.getInputStream());
			}catch(IOException e){
				TemporaryMessageException e1 = new TemporaryMessageException("Temporary Error Occurs; save message to queue("+queue.getName()+")", e); //TODO: ??现在简单处理，让客户端重试；正确做法应该是让客户端死信；
				log.error(e1.getMessage(), e);
				throw e1;
			}
			return new MsgSuccessConfirm();
		}


		private void sendErrorResponse( HttpServletResponse response, int statusCode, String description) {
			response.setStatus(statusCode);
			try{
				byte[] b = description.getBytes(HttpConstants.CHARSET_UTF8);
				response.setContentLength(b.length);
				
				response.getOutputStream().write(b);
				response.getOutputStream().flush();
			}catch(Exception e1){
				log.error("failed to send error response", e1);
			}
		}
	}


}
