package com.hong.dip.smq;

import java.io.IOException;

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
}
