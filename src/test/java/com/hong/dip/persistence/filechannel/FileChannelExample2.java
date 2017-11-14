package com.hong.dip.persistence.filechannel;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.file.FileChannel;
import org.apache.flume.channel.file.FileChannelConfiguration;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

public class FileChannelExample2 {

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
	
	public FileChannelExample2() {
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
		context.put(FileChannelConfiguration.CAPACITY, String.valueOf(10000000));
		context.put(FileChannelConfiguration.TRANSACTION_CAPACITY,String.valueOf(10000000));
		context.putAll(overrides);
		return context;
	}
	
	//放入多条数据
	public void putMoreData(){
		long start = System.currentTimeMillis();
		int num = 1000000;
		Transaction transaction = channel.getTransaction();
        transaction.begin();
        //批量插入
        for (int i = 0; i < num; i++) {
            String eventData = (""+i+UUID.randomUUID()).toString();
            Event event = new SimpleEvent();
            event.setBody(eventData.getBytes());
            channel.put(event);
        }
        transaction.commit();
        transaction.close();
        long end = System.currentTimeMillis();
        System.out.println("put "+num+"条数据，耗时"+(end-start));
        channel.stop();//不调用stop的话，JVM不会退出，此句可以用在hong dip 停止的时候
	}
	
	//h获取多条数据
	public void takeMoreData(){
		long start = System.currentTimeMillis();
		int num = 1000000;
        //此处模拟另外的一个读取端
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        Set<String> events = Sets.newHashSet();
        for (int i = 0; i < num; i++) {
            Event e = channel.take();
            if (e == null) {
                break;
            }
            events.add(new String(e.getBody()));
        }
        transaction.commit();//从队列中pop()出数据，否则只是peek()
        transaction.close();
        long end = System.currentTimeMillis();
        System.out.println("take "+events.size()+"条数据，耗时"+(end-start));
        channel.stop();//不调用stop的话，JVM不会退出，此句可以用在hong dip 停止的时候
	}
	
	
	public static void main(String[] args) {
		//先执行putMoreData(),退出虚拟机后，打开take的代码，关闭put的代码，测试其持久化特性。
		//new FileChannelExample2().putMoreData();
		new FileChannelExample2().takeMoreData();
	}

}
