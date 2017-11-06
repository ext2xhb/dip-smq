package com.hong.dip.smq.transport.http;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

public class SimpleTest {
    public static void main( String[] args ) throws Exception
    {
        Server server = new Server(8080);

        ContextHandler context = new ContextHandler();
        context.setContextPath("/queue");
        context.setHandler(new HelloHandler("Root Hello"));
        /*
        ContextHandler contextFR = new ContextHandler("/fr");
        contextFR.setHandler(new HelloHandler("Bonjoir"));

        ContextHandler contextIT = new ContextHandler("/it");
        contextIT.setHandler(new HelloHandler("Bongiorno"));
	*/
       
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        contexts.addHandler(context);
      //  contexts.addHandler(contextFR);
        //contexts.addHandler(contextIT);
        //contexts.setHandlers(new Handler[] { context, contextFR, contextIT});

        server.setHandler(contexts);

        server.start();
        server.join();
   }

}
