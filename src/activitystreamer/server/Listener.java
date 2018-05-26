package activitystreamer.server;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.util.Settings;

public class Listener extends Thread{
	private static final Logger log = LogManager.getLogger();
	private ServerSocket serverSocket;
	private boolean term = false;
	private int portNum;
	
	public Listener() throws IOException{
		portNum = Settings.getLocalPort(); // keep our own copy in case it changes later
		log.debug("In Coming Address: " + portNum);
		serverSocket = new ServerSocket(portNum);
		start();
	}
	
	@Override
	public void run() {
		System.out.println("Listener Started.");
		while(!term){
			Socket clientSocket;
			try {
				clientSocket = serverSocket.accept();
				Control.getInstance().incomingConnection(clientSocket);
			} catch (IOException e) {
				log.info("received exception, shutting down");
				term=true;
			}
		}
	}

	public void setTerm(boolean term) {
		this.term = term;
		if(term) interrupt();
	}
	
	
}
