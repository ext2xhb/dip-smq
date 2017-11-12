package com.hong.dip.persistence.filechannel;

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
import com.hong.dip.smq.storage.flume.FlumeOptions;
import com.hong.dip.smq.storage.flume.FlumeQueueStorage;
import com.hong.dip.smq.storage.flume.FlumeStorage;

import junit.framework.TestCase;

public class TestChannelExample extends TestCase{

	private FileChannel channel;
	private File baseDir;
	private File checkpointDir;
	private File[] dataDirs;
	private String dataDir;
	private File backupDir;
	private Map<String, String> overrides;

	//还可以进行压缩子类的，个人认为没什么用，先不管
/*	protected File uncompressedBackupCheckpoint;
	protected File compressedBackupCheckpoint;*/
	
	public TestChannelExample() {
		this.initChannel();
		this.startChannel();
	}
	
	public void initChannel() {
		baseDir = Files.createTempDir();// TODO:此处可以根据实际情况修改，也可以使用该值（路径为System.getProperty("java.io.tmpdir"）
		baseDir = new File("c:\\Temp\\channel");
		checkpointDir = new File(baseDir, "chkpt"); // WLA 模式中的checkpoint的存放处
		backupDir = new File(baseDir, "backup");// WLA 模式中的backup的存放处
		checkpointDir.mkdirs();
		backupDir.mkdirs();
		dataDirs = new File[3];// WLA 模式中的data的存放处
		dataDir = "";
		for (int i = 0; i < dataDirs.length; i++) {
			dataDirs[i] = new File(baseDir, "data" + (i + 1));
			dataDirs[i].mkdirs();
			dataDir += dataDirs[i].getAbsolutePath() + ",";
		}
		dataDir = dataDir.substring(0, dataDir.length() - 1);
		overrides = new HashMap<String, String>();// 此处可以扩展的其他属性
		channel = createFileChannel(checkpointDir.getAbsolutePath(), dataDir,
				backupDir.getAbsolutePath(), overrides);
	}
	
	public void startChannel() {
		this.channel.start();
	}
	
	public void stopChannel() {
		this.channel.stop();
	}

	private FileChannel createFileChannel(String checkpointDir, String dataDir,
			String backupDir, Map<String, String> overrides) {
		FileChannel channel = new FileChannel();
		channel.setName("FileChannel-" + UUID.randomUUID());
		Context context = createFileChannelContext(checkpointDir, dataDir,
				backupDir, overrides);
		Configurables.configure(channel, context);
		return channel;
	}

	private Context createFileChannelContext(String checkpointDir,
			String dataDir, String backupDir, Map<String, String> overrides) {
		Context context = new Context();
		context.put(FileChannelConfiguration.CHECKPOINT_DIR, checkpointDir);
		if (backupDir != null) {
			context.put(FileChannelConfiguration.BACKUP_CHECKPOINT_DIR,
					backupDir);
		}
		context.put(FileChannelConfiguration.DATA_DIRS, dataDir);
		context.put(FileChannelConfiguration.KEEP_ALIVE, String.valueOf(1));
		context.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000));
		//context.put(FileChannelConfiguration.TRANSACTION_CAPACITY,String.valueOf(10000000));
		context.putAll(overrides);
		return context;
	}
	
	//放入并获取一条数据
	public void putAndTakeSingleData(){
		Transaction transaction = channel.getTransaction();
        transaction.begin();
        Event event = new SimpleEvent();
        event.setBody("this is a test ".getBytes());
        channel.put(event);
        //查询可能会在另外的逻辑中，为了模拟真实的场景，先提交并关闭事务。
        transaction.commit();
        transaction.close();
        
        //此处模拟另外的一个读取端
        Transaction transaction1 = channel.getTransaction();
        transaction1.begin();
        Event event1 =  channel.take();
        transaction1.commit();//从队列中pop()出数据，否则只是peek()
        transaction1.close();
        System.out.println(new String(event1.getBody()));
        channel.stop();//不调用stop的话，JVM不会退出，此句可以用在hong dip 停止的时候
	}
		
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
		options.setTransactionCapacity(2);
		FlumeStorage flumeStorage = new FlumeStorage(options);
		flumeStorage.start();
		FlumeQueueStorage queueStorage = (FlumeQueueStorage)flumeStorage.getOrCreateQueueStorage("q1");
		int size = consumeAll(queueStorage.getChannel());
		System.out.println("consumed = " + size);
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
	}

	private static int consumeAll(FileChannel channel) {
		
		Event e;
		int size = 0;
		do{
			Transaction t = channel.getTransaction();
			t.begin();
			e = channel.take();
			e = channel.take();
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
