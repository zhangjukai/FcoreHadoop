package com.fcore.hadoop.test;

import java.text.MessageFormat;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.fcore.hadoop.utils.HttpClientUtil;

public class WebHdfsTest {
	private static Logger logger = Logger.getLogger(WebHdfsTest.class);  
	private String url = "http://192.168.182.128:50070/webhdfs/v1//home/hdp/hadoop/hdfs/name?op=LISTSTATUS";
	private HttpClientUtil clientUtil = null;
	
	@Before
	public void init() {
		clientUtil = HttpClientUtil.getInstance();
	}
	
	@Test
	public void test(){
		String httpUrl = MessageFormat.format("http://192.168.182.128:50070/webhdfs/v1/{0}?op=LISTSTATUS", "home/hdp/hadoop/hdfs/name");
		System.out.println(clientUtil.sendHttpGet(httpUrl));
	}
	
}
