package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import activitystreamer.util.Settings;

public class ClientSkeleton extends Thread {
	private static final Logger log = LogManager.getLogger();
	private static ClientSkeleton clientSolution;
	private TextFrame textFrame;
	
	private Socket serverSocket;
	private DataOutputStream outStream;
	private DataInputStream inStream;
	private BufferedReader inReader;
	private PrintWriter outWriter;
	private JSONParser parser;
	

	
	public static ClientSkeleton getInstance(){
		if(clientSolution==null){
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}
	
	
	//Create instance and create connection with the provided server info
	public ClientSkeleton() {
	    try {
	        serverSocket = new Socket(Settings.getRemoteHostname(), Settings.getRemotePort());
		    inStream = new DataInputStream(serverSocket.getInputStream());
		    outStream = new DataOutputStream(serverSocket.getOutputStream());
		    inReader = new BufferedReader( new InputStreamReader(inStream));
		    outWriter = new PrintWriter(outStream, true);
	    }
	    catch (IOException e) {
	        System.out.println(e);
	    }
		parser = new JSONParser();
		
		textFrame = new TextFrame();
		start();
	}
	
	
	@SuppressWarnings("unchecked")
	public void sendActivityObject(JSONObject activityObj){
		outWriter.print(activityObj);
		outWriter.flush();
	}
	
	
	public void disconnect(){
		try {
			inReader.close();
			outWriter.close();
		} catch (IOException e) {
			log.error("error closing socket: " + e);
		}
		
	}
	
	//Process all incoming message from server
	public void run(){
		try {
			JSONObject json;
			String data;
			while((data = inReader.readLine())!=null){
				try {
					json = (JSONObject) parser.parse(data);
					textFrame.setOutputText(json);
				} catch (ParseException pe) {
					log.error("error in parsing string :"+pe);
				}
			}
		} catch (IOException e) {
			log.error("connection closed with exception: "+e);
		}
	}
	
	
	//Reconnect to other server with provided info, return true if
	//create success
	//private boolean reconnect(JSONObject response)
	
	
	
	//Used to check the integrity of the server response, true if consistent 
	//private boolean checkMessageIntegrity(JSONObject response)

	
}
