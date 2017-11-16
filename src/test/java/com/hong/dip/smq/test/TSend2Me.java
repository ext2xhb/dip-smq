package com.hong.dip.smq.test;

import java.io.File;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.ChunkableDataSource;
import com.hong.dip.smq.Message;
import com.hong.dip.smq.MessagePostHandler;
import com.hong.dip.smq.MessageReason;
import com.hong.dip.smq.Node;
import com.hong.dip.smq.Queue;
import com.hong.dip.smq.QueueServer;
import com.hong.dip.smq.RemoteQueue;
import com.hong.dip.smq.storage.flume.FlumeOptions;
import com.hong.dip.utils.StringUtils;

public class TSend2Me extends SpringTestSupport{
	Logger log = LoggerFactory.getLogger(TSend2Me.class);
	QueueServer rNode;
	
	Queue recvQ;
	RemoteQueue sendQueue;
	@Override
	public String[] getSpringConfig() {
		return new String[]{"TestSimpleSndRecv_Server.xml"};
	}
	public TSend2Me() throws Exception{
		PropertyConfigurator.configure(this.getClass().getClassLoader().getResource("log4jRecv.properties").toURI().toURL());
		this.getSpringContext();
		rNode = getQServer("rNode");

		recvQ = rNode.createQueue("Simple");
		sendQueue = rNode.createRemoteQueue("Simple", new Node("rNode", "127.0.0.1", 8081),
			new MessagePostHandler(){
				@Override
				public boolean needFullMessage() {
					return true;
				}
	
				@Override
				public void handle(MessageReason reason, Message msg) {
					System.out.println("message ("+msg.getID()+") sended"); 
				}
			
			});
	}
	
	QueueServer getQServer(String node){
		return this.getBean(node, QueueServer.class);
	}
	public void testSendInfinite() throws Exception{

		while(true){
			Message mSend = sendQueue.createMessage();
			mSend.setByteBody("Hello world".getBytes());
			sendQueue.putMessage(mSend);
			sendQueue.commit();
		}
	}
	
	public void testReceiveInfinite() throws Exception{
		while(true){
			Message mRecv = recvQ.getMessage(Integer.MAX_VALUE);
			System.out.println("msg received " + mRecv.getID());
			assertTrue(StringUtils.byteArrayEquals(mRecv.getByteBody(), "hello world".getBytes()));
			if(mRecv.getAttachments().size() == 3){
				for(ChunkableDataSource attach : mRecv.getAttachments()){
					if(attach.getName().equals("chunk0")){
						assertTrue(attach.getSize() == 0);
						attach.cleanSource();
					}else if(attach.getName().equals("chunk1")){
						if(attach.getSize() != 6)
							log.error("" + attach.getName() + " size " + attach.getSize() + " Error");
						attach.cleanSource();
					}else{
						if(attach.getSize() != 7)
							log.error("" + attach.getName() + " size " + attach.getSize() + " Error");
						attach.cleanSource();
					}
				}
			}else{
				log.error("attachment size != 3, msgid = " + mRecv.getID());
			}
			recvQ.commit();
		}
		
	}
	public static void main(String[] args) throws Exception{
		new Thread(new Runnable(){

			@Override
			public void run() {
				try{
				new TSend2Me().testSendInfinite();
				}catch(Exception e){
					e.printStackTrace();;
				}
			}
			
		}).start();
		new TSend2Me().testReceiveInfinite();
	}

}
