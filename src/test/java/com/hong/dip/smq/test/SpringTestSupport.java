package com.hong.dip.smq.test;

import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import junit.framework.TestCase;

public abstract class SpringTestSupport extends TestCase{
	AbstractXmlApplicationContext context;
	
	public abstract String[] getSpringConfig();
	public AbstractXmlApplicationContext getSpringContext(){
		if(context == null)
			context = new ClassPathXmlApplicationContext(this.getSpringConfig());
		return context;
	}
	public <T> T getBean(String name, Class<T> cls){
		return this.getSpringContext().getBean(name, cls);
	}
	public <T> T getBean(Class<T> cls){
		return this.getSpringContext().getBean(cls);
	}
	
	public SpringTestSupport(){
		
		
	}
}
