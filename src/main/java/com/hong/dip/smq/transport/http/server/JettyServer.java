package com.hong.dip.smq.transport.http.server;

import java.io.Closeable;
import java.io.IOException;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;

import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.camel.support.ServiceSupport;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.ErrorHandler;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hong.dip.utils.StringUtils;

class JettyServer extends ServiceSupport{
	
    static final private Logger log = LoggerFactory.getLogger(JettyServer.class); 

    private JettyOptions options;
	private Server server;
    private ServerConnector connector;
    
    private ContextHandlerCollection contexts;
	private ServletHandler servletHander;
    
    
	public JettyServer(JettyOptions options){
		this.options = options;
	}
	
	public void addServant(String path, AbstractHandler handler) throws Exception{
		path = formatPath(path);
		
		ContextHandler context = new ContextHandler();
        context.setContextPath(path);
        context.setHandler(handler);
        contexts.addHandler(context);
        
        if (contexts.isStarted()) {
            try {
                context.start();
            }catch(Exception e){
            	log.error("cannot start a new context handler for path=" + path, e);
            	throw e;
            }
        }
		
	}

	private String formatPath(String path) {
		if(path.length() == 0 || path.charAt(0) != '/'){
			path = "/" + path;
		}
		return path;
	}
	public void removeServant(String path) {
		path = formatPath(path);
        for (Handler handler : contexts.getChildHandlersByClass(ContextHandler.class)) {
            ContextHandler contextHandler = null;
            if (handler instanceof ContextHandler) {
                contextHandler = (ContextHandler) handler;
                if (path.equals(contextHandler.getContextPath())){
                    try {
                        contexts.removeHandler(handler);
                        handler.stop();
                        handler.destroy();
                    } catch (Exception ex) {
                        log.warn("Failed to remove handler for path(" + path+")", ex);
                    }
                    break;
                }
            }
        }
	}

	@Override
	protected void doStart() throws Exception {
        server = createServer();
        enableMBean(server);
        
        connector = createConnector(options.getHost(), options.getPort());
        
        server.addConnector(connector);
        setupThreadPool();
        
        initializeHandler();
        
        try {
            server.start();
        } catch (Exception e) {
            log.error("START_UP_SERVER_FAILED_MSG(" + options.getHost()+ ":" + options.getPort() + ")", e);
            //problem starting server
            try {
                server.stop();
                server.destroy();
            } catch (Exception ex) {
              
            }
            server = null;
            throw e;
        }
	}

	private void initializeHandler() {
		
		contexts = new ContextHandlerCollection();
		server.setHandler(contexts);
        //HandlerCollection handlerCollection = new HandlerCollection();
        //handlerCollection.addHandler(contexts);
       // handlerCollection.addHandler(new DefaultHandler());
       //server.setHandler(handlerCollection);
        /*
        servletHander = new ServletHandler();
		server.setHandler(servletHander);
		*/
	}
	
	@Override
	protected void doStop() throws Exception {
        if (server != null) {
            try {
                if (connector != null) {
                    connector.stop();
                    if (connector instanceof Closeable) {
                        ((Closeable)connector).close();
                    } else {
                        ((ServerConnector)connector).close();
                    }
                }
            } finally {
                if (contexts != null) {
                	if(contexts.getHandlers() != null){
	                    for (Handler h : contexts.getHandlers()) {
	                        h.stop();
	                    }
                	}
                    contexts.stop();
                }
                contexts = null;
                server.stop();
                removeMBean(server);
                server.destroy();
                server = null;
            }
        }
		
	}
	private void setupThreadPool()  throws Exception{
		QueuedThreadPool pool = (QueuedThreadPool)server.getThreadPool();
        if (pool == null) {
            pool = new QueuedThreadPool();
            try {
                server.getClass().getMethod("setThreadPool", ThreadPool.class).invoke(server, pool);
            } catch (Throwable t) {
                throw new Exception("Cannot set ThreadPool", t);
                
            }
        }
        int acc = connector.getAcceptors() * 2;
        pool.setName(options.getThreadPoolName());
        if(options.getMinThreads() > 0)
        	pool.setMinThreads(options.getMinThreads());
        if(options.getMaxThreads() > 0)
        	pool.setMaxThreads(options.getMaxThreads() + acc);
		
	}
	private ServerConnector createConnector(String host, int port) throws Exception{
        ServerConnector result = null;
        try {
            HttpConfiguration httpConfig = new HttpConfiguration();
            httpConfig.setSendServerVersion(options.getSendServerVersion());
            HttpConnectionFactory httpFactory = new HttpConnectionFactory(httpConfig);
            Collection<ConnectionFactory> connectionFactories = new ArrayList<>();
            connectionFactories.add(httpFactory);

            result = new org.eclipse.jetty.server.ServerConnector(server);
            result.setConnectionFactories(connectionFactories);

            if (options.getMaxIdleTime() > 0) {
                result.setIdleTimeout(Long.valueOf(options.getMaxIdleTime()));
            }
    	    if (host != null) {
                result.setHost(host);
            }
    	    result.setPort(port);
            result.setReuseAddress(options.isReuseAddress()); 
            log.info("connector.host: {} connector.port:{} ", result.getHost(), result.getPort());

        } catch (Throwable t) {
        	log.error("failed to create connector", t);
            throw new Exception(t);
        }
        return result;
	}
	private void enableMBean(Server server ) {
		//TODO  enable JMX functions
	}
	private void removeMBean(Server server) {
		//TODO clean mbeans
	}
	
	
    private Server createServer() {
        Server s = new Server();

        // need an error handler that won't leak information about the exception
        // back to the client.
        ErrorHandler eh = new ErrorHandler() {
            public void handle(String target, Request baseRequest, HttpServletRequest request,
                               HttpServletResponse response) throws IOException {
                String msg = (String)request.getAttribute(RequestDispatcher.ERROR_MESSAGE);
                if (StringUtils.isEmpty(msg) /*|| msg.contains("org.apache.cxf.interceptor.Fault")*/) {
                    msg = HttpStatus.getMessage(response.getStatus());
                    request.setAttribute(RequestDispatcher.ERROR_MESSAGE, msg);
                }
                if (response instanceof Response) {
                    ((Response)response).setStatusWithReason(response.getStatus(), msg);
                }
                super.handle(target, baseRequest, request, response);
            }

            protected void writeErrorPage(HttpServletRequest request, Writer writer, int code, String message,
                                          boolean showStacks) throws IOException {
                super.writeErrorPage(request, writer, code, message, false);
            }
        };
        s.addBean(eh);
        return s;
    }

	public void join() throws InterruptedException {
		if(server != null && server.isStarted())
			server.join();
	}

   
}
