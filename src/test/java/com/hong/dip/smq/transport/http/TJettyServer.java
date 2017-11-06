package com.hong.dip.smq.transport.http;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;

public class TJettyServer {
	static public void main(String[] args) throws Exception {
		JettyOptions options = new JettyOptions();
		//options.setMaxIdleTime(5000);
		JettyServer server = new JettyServer(options);
		server.start();
		server.addServant("/queue/*", new TestHandler());
		
		
		/*
		server.addServlet("/queue/*", TestServlet.class);
		server.addServlet("/*", TestServlet2.class);
		*/
		server.join();

	}

	public static class TestServlet extends HttpServlet {
		protected void doGet(HttpServletRequest request, HttpServletResponse response)
				throws ServletException, IOException {
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().println("<h1>Hello from HelloServlet " + request.getRequestURI() + ": " + request.getContextPath() + "</h1>");
		}
	}
	public static class TestServlet2 extends HttpServlet {
		protected void doGet(HttpServletRequest request, HttpServletResponse response)
				throws ServletException, IOException {
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().println("<h1>Hello from HelloServlet2 " + request.getRequestURI() + ": " + request.getContextPath() +  "</h1>");
		}
	}
	
	public static class TestHandler extends AbstractHandler{

		@Override
		public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
				throws IOException, ServletException {
			
			response.setContentType("text/html");
			response.setStatus(HttpServletResponse.SC_OK);
			response.getWriter().println("<h1>Hello from HelloHandler target=" + target + ":uri=" +  request.getRequestURI() + ": ctx=" + request.getContextPath() + "</h1>");
			baseRequest.setHandled(true);
		}
		
	}

}
