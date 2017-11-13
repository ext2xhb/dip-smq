package com.hong.dip.smq.test;

import java.io.File;
import java.io.IOException;

import com.hong.dip.smq.Message;
import com.hong.dip.smq.Node;
import com.hong.dip.smq.Queue;
import com.hong.dip.smq.QueueServer;
import com.hong.dip.smq.RemoteQueue;
import com.hong.dip.smq.storage.flume.FlumeOptions;
import com.hong.dip.utils.StringUtils;

public class TSimpleSender extends SpringTestSupport{

	QueueServer sNode;
	
	Queue recvQ;
	@Override
	public String[] getSpringConfig() {
		return new String[]{"TestSimpleSndRecv_Client.xml"};
	}
	public TSimpleSender() throws Exception{
		this.getSpringContext();
		sNode = getQServer("sNode");
	}
	
	QueueServer getQServer(String node){
		return this.getBean(node, QueueServer.class);
	}
	public void testSendInfinite() throws Exception{
		int chunkSize = this.getSpringContext().getBean("sNode_Flume", FlumeOptions.class).getChunkSize();

		File chunk0 = prepareAttachmentFile("a_"+chunkSize*0, chunkSize*0, 'a');
		File chunk1 = prepareAttachmentFile("a_"+chunkSize*2, chunkSize*2, 'a');
		File chunk2 = prepareAttachmentFile("a_"+(chunkSize*2+1), chunkSize*2+1, 'a');

		RemoteQueue senderQ = sNode.createRemoteQueue("Simple", new Node("rNode", "127.0.0.1", 8081));
		while(true){
			
			Message mSend = senderQ.createMessage();
			mSend.setByteBody("hello world".getBytes());
			mSend.addAttachment("chunk0", chunk0);
			mSend.addAttachment("chunk1", chunk1);
			mSend.addAttachment("chunk2", chunk2);
			
			senderQ.putMessage(mSend);
			senderQ.commit();
		}
		
	}
	private File prepareAttachmentFile(String name, int size, char c) throws IOException {
		File file = new File("./log/"+name);
		StringUtils.touchFile(file, size, c);
		return file;
	}
}
