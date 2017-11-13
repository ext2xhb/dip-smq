package com.hong.dip.utils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import junit.framework.TestCase;

public class TestStringUtils extends TestCase{
	
	public void testList2String(){
		List<String> list = Arrays.asList("a", "b", "c");
		String s = StringUtils.list2String(list);
		List<String> list2 = StringUtils.string2List(s);
		List<String> list3 = StringUtils.string2List(s.substring(0, s.length() - 1));
		
		assertEquals(list, list2);
		assertEquals(list, list3);
		
		String s2 = StringUtils.list2String(list2);
		assertEquals(s, s2);
	}
	public void testMkEmptyFile() throws Exception{
		File file = new File(".\\snode_flume\\attachment\\__check_log_");
		file.delete();
		StringUtils.mkEmptyFile(file);;
		assertTrue(file.exists());
	}
}