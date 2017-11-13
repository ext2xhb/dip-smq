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

public class TestSimpleSndRecv extends SpringTestSupport{

	QueueServer sNode, sNode2, rNode;
	
	Queue recvQ;
	@Override
	public String[] getSpringConfig() {
		return new String[]{"TestSimpleSndRecv_Client.xml", "TestSimpleSndRecv_Client2.xml", "TestSimpleSndRecv_Server.xml"};
	}
	public TestSimpleSndRecv() throws Exception{
		this.getSpringContext();
		sNode = getQServer("sNode");
		sNode2 = getQServer("sNode2");
		rNode = getQServer("rNode");

		recvQ = rNode.createQueue("Simple");
		while(recvQ.getStorage().take(0) != null){
			recvQ.commit();
		}
	}
	
	QueueServer getQServer(String node){
		return this.getBean(node, QueueServer.class);
	}
	public void _testSimpleSendReceive() throws Exception{
		RemoteQueue senderQ = 
				sNode.createRemoteQueue("Simple", new Node("rNode", "127.0.0.1", 8081));
		
		while(senderQ.getStorage().take(0) != null){
			senderQ.commit();
		}
		

		Message mSend = senderQ.createMessage();
		mSend.setByteBody("hello world".getBytes());
		senderQ.putMessage(mSend);
		senderQ.commit();
		mSend.setID(null);
		mSend.setByteBody(null);
		senderQ.putMessage(mSend);
		senderQ.commit();
		
		
		Message mRecv = recvQ.getMessage(Integer.MAX_VALUE);
		recvQ.commit();
		assertTrue(mRecv != null);
		assertTrue(StringUtils.byteArrayEquals(mRecv.getByteBody(), "hello world".getBytes()));
		
		mRecv = recvQ.getMessage(Integer.MAX_VALUE);
		recvQ.commit();
		assertTrue(mRecv != null);
		assertTrue(mRecv.getByteBody().length == 0);
		mRecv = recvQ.getMessage(1000);
		assertTrue(mRecv == null);
		
	}
	public void testSimpleAttachment() throws Exception{
		int chunkSize = this.getSpringContext().getBean("sNode2_Flume", FlumeOptions.class).getChunkSize();
		RemoteQueue senderQ2 = 
				sNode2.createRemoteQueue("Simple", new Node("rNode", "127.0.0.1", 8081));
		
		while(senderQ2.getStorage().take(0) != null){
			senderQ2.commit();
		}
		
		File chunk0 = prepareAttachmentFile("a_"+chunkSize*0, chunkSize*0, 'a');
		
		File chunk1 = prepareAttachmentFile("a_"+chunkSize*2, chunkSize*2, 'a');
		File chunk2 = prepareAttachmentFile("a_"+(chunkSize*2+1), chunkSize*2+1, 'a');
		
		Message mSend = senderQ2.createMessage();
		mSend.setByteBody("aaa".getBytes());
		mSend.addAttachment("chunk0", chunk0);
		mSend.addAttachment("chunk1", chunk1);
		mSend.addAttachment("chunk2", chunk2);
		
		senderQ2.putMessage(mSend);
		senderQ2.commit();
		Message mRecv = recvQ.getMessage(Integer.MAX_VALUE);
		assertEquals(mRecv.getID(),mSend.getID());
		assertTrue(mRecv.getAttachment("chunk0").getSize() == chunkSize*0);
		assertTrue(mRecv.getAttachment("chunk1").getSize() == chunkSize*2);
		assertTrue(mRecv.getAttachment("chunk2").getSize() == chunkSize*2+1);
		
	}
	private File prepareAttachmentFile(String name, int size, char c) throws IOException {
		File file = new File("./log/"+name);
		StringUtils.touchFile(file, size, c);
		return file;
	}
}
