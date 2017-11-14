package com.hong.dip.smq.storage.flume;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;

import com.google.common.io.Files;
import com.hong.dip.smq.storage.MessageStorage;
import com.hong.dip.smq.storage.flume.FlumeMessageStorage;
import com.hong.dip.smq.storage.flume.FlumeOptions;
import com.hong.dip.smq.storage.flume.FlumeQueueStorage;
import com.hong.dip.smq.storage.flume.FlumeStorage;

import junit.framework.TestCase;

public class TestChannelExample extends TestCase{

	public static void main(String[] args) throws Exception{
		//testTakeFlumeStorage();
		testSaveFlumeStorage();
		//testTakeFlumeStorage();
		
		//new TestChannelExample().putAndTakeSingleData();
	}
	static public void testSaveFlumeStorage() throws Exception{
		FlumeOptions options = new FlumeOptions("D:\\temp");
		options.setTransactionCapacity(3);
		options.setPutWaitSeconds(3);
		options.setDefaultQueueDepth(10);
		options.setTransactionCapacity(10);
		FlumeStorage flumeStorage = new FlumeStorage(options);
		flumeStorage.start();
		FlumeQueueStorage queueStorage = (FlumeQueueStorage)flumeStorage.getOrCreateQueueStorage("q1");
		
		for(int i = 0 ;  i< 3; i++){
		execute(new Runnable(){

			@Override
			public void run() {
				try{
					FlumeMessageStorage msg = new FlumeMessageStorage();
					queueStorage.offer(msg);
					queueStorage.commit();
				}catch(Exception e){
					
				}
				
			}
			
		});
		}
		execute(new Runnable(){

			@Override
			public void run() {
				try {
					while(true){
						MessageStorage msg = queueStorage.take(Integer.MAX_VALUE);
						if(msg != null)
							queueStorage.commit();

					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}//consumeAll(queueStorage.getChannel());
			}
			
		});
		
		System.out.println("ok");
		/*
		Transaction transaction = queueStorage.getChannel().getTransaction();
		transaction.begin();
		queueStorage.getChannel().put(new SimpleEvent());
	
		transaction.commit();
		transaction.close();
		
		transaction = queueStorage.getChannel().getTransaction();
		transaction.begin();
		
		queueStorage.getChannel().put(new SimpleEvent());
		transaction.commit();
		transaction.close();
		*/
	}
	static void execute(Runnable run){
		new Thread(run).start();
	}
	private static int consumeAll(FlumeQueueStorage queueStorage) throws Exception {
		MessageStorage msg;
		int size = 0;
		while((msg = queueStorage.take(0)) != null){
			queueStorage.commit();
			size ++;
		}
		return size;
	}

	private static int consumeAll(FileChannel channel) {
		
		Event e;
		int size = 0;
		do{
			Transaction t = channel.getTransaction();
			t.begin();
			e = channel.take();
			if(e != null)
				size++;
			t.commit();
			t.close();
			
		}while(e != null);
		return size;
	}

	static public void testTakeFlumeStorage() throws Exception{
		FlumeOptions options = new FlumeOptions("D:\\temp");
		options.setTransactionCapacity(10);
		options.setPutWaitSeconds(3);
		options.setDefaultQueueDepth(1);
		FlumeStorage flumeStorage = new FlumeStorage(options);
		flumeStorage.start();
		FlumeQueueStorage queueStorage = (FlumeQueueStorage)flumeStorage.getOrCreateQueueStorage("q1");
		Transaction transaction = queueStorage.getChannel().getTransaction();
		transaction.begin();
		Event event = queueStorage.getChannel().take();
		transaction.commit();
		transaction.close();
		
		transaction = queueStorage.getChannel().getTransaction();
		transaction.begin();
		event = queueStorage.getChannel().take();
		transaction.commit();
		transaction.close();
	}

}
