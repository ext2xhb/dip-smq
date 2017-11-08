package com.hong.dip.smq.transport.http;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.Queue;
import com.hong.dip.smq.transport.ServerTransport;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgCheckResult;
import com.hong.dip.smq.transport.http.MsgTransportCmd.MsgPostConfirm;

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
	public void registerMessageReceiver(Queue queue) throws Exception {
		try{
			server.addServant("/queue/" + queue.getName(), new JettyHandler(queue));
		}catch(Exception e){
			log.error("cannot add url handler for queue " + queue.getName(), e);
			throw e;
		}
	}
	
	class JettyHandler extends AbstractHandler{
		private Queue queue;
		public JettyHandler(Queue queue){
			this.queue = queue;
		}
		@Override
		public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response){
			try{
				MsgTransportCmd cmd = MsgTransportCmd.buildCmdForQueue(request, queue);
				if(cmd == null){
					String err = "url(" + request.getContextPath() + ") mismatch queue(" + queue.getName()+ ")";
					log.error(err);
					sendErrorResponse(response, new HttpStatusException(HttpConstants.STATUS_CONFLICT,err));
				}
				switch(cmd.getCmdType()){
				case MSG_POST:
					MsgTransportCmd.MsgPostConfirm confirm = executeMsgPost(cmd, queue, request);
					sendPostConfirm(response, confirm);
					break;
				case MSG_CHECK:
					MsgTransportCmd.MsgCheckResult result = executeMsgCheck(cmd, queue);
					sendCheckResult(response, result);
					break;
				default:
					String err = "unkown transport cmd: " + request.toString();
					log.error(err);
					sendErrorResponse(response, new HttpStatusException(HttpConstants.STATUS_CONFLICT, "unknown transport cmd"));
					break;
				}
				
			}finally{
				baseRequest.setHandled(true);
			}
		}

		private void sendCheckResult(HttpServletResponse response, MsgCheckResult result) {
			// TODO Auto-generated method stub
			
		}
		private MsgCheckResult executeMsgCheck(MsgTransportCmd cmd, Queue queue2) {
			// TODO Auto-generated method stub
			return null;
		}
		private void sendPostConfirm(HttpServletResponse response, MsgPostConfirm confirm) {
			// TODO Auto-generated method stub
			
		}
		private MsgPostConfirm executeMsgPost(MsgTransportCmd cmd, Queue queue2, HttpServletRequest request) {
			// TODO Auto-generated method stub
			return null;
		}

		private void executeCmd(MsgTransportCmd cmd, Queue queue, HttpServletRequest request) {
			
		}
		private void sendErrorResponse( HttpServletResponse response, HttpStatusException e) {
			response.setStatus(e.getStatusCode());
			try{
				response.getOutputStream().write(e.getMessage().getBytes(HttpConstants.CHARSET_UTF8));
				response.getOutputStream().flush();
			}catch(Exception e1){
				log.error("failed to send error response", e1);
			}
		}
	}

}
