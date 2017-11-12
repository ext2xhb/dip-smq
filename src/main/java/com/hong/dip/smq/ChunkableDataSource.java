package com.hong.dip.smq;

import java.io.IOException;
import java.nio.ByteBuffer;

import javax.activation.DataSource;

public interface ChunkableDataSource extends DataSource{
	
	/**
	 * 返回数据源的大小
	 * @return: datasource's length
	 */
	long getSize();
	
	/**
	 * 清除数据源的内容
	 */
	void cleanSource() throws IOException;
	
	/**
	 * @return true: 有效地数据源，false：无效数据源
	 */
	boolean isValid();
	
	/**
	 * open a reader to read chunk
	 * @return
	 * @throws IOException
	 */
	public ChunkReader openReader() throws IOException;
	
	public interface ChunkReader{
		
		/**
		 * 读取一块内容。读取内容一般都会保存在传入参数ByteBuffer中，也可以自行决定使用自己内部的Buffer。
		 * 返回的Buffer可以直接读取，无需再执行Flip
		 * @param buffer：读取结果需要保存的ByteBuffer，
		 * @return：null：读取结束; 非NULL：记录成功读取内容的ByteBuffer，
		 * @throws IOException
		 */
		public ByteBuffer readChunk(ByteBuffer buffer) throws IOException;
		/**
		 * close reader;
		 */
		public void close();
		
		/**
		 * 将读取的位置移动到chunkIdx的位置处
		 * @param chunkIdx	
		 * @param chunkSize（Chunk块的最大值）
		 * @throws IOException	chunk位置溢出则抛异常
		 */
		public void pos(long chunkIdx, int chunkSize) throws IOException;
		/**
		 * reader的最大长度
		 * @return
		 */
		public long getSize() throws IOException;
	}
}
