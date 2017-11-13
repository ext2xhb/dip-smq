package com.hong.dip.smq.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.smq.ChunkableDataSource;
import com.hong.dip.smq.Message;
import com.hong.dip.smq.Queue;
import com.hong.dip.smq.QueueServer;
import com.hong.dip.utils.StringUtils;

public class TSimpleReceiver extends SpringTestSupport{
	Logger log = LoggerFactory.getLogger(TSimpleReceiver.class);
	QueueServer rNode;
	
	Queue recvQ;
	@Override
	public String[] getSpringConfig() {
		return new String[]{"TestSimpleSndRecv_Server.xml"};
	}
	public TSimpleReceiver() throws Exception{
		this.getSpringContext();
		rNode = getQServer("rNode");

		recvQ = rNode.createQueue("Simple");
		while(recvQ.getStorage().take(0) != null){
			recvQ.commit();
		}
	}
	
	QueueServer getQServer(String node){
		return this.getBean(node, QueueServer.class);
	}
	public void testReceiveInfinite() throws Exception{
		while(true){
			Message mRecv = recvQ.getMessage(Integer.MAX_VALUE);
			recvQ.commit();
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
		}
		
	}


}
