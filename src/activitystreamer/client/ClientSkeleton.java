package activitystreamer.client;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;

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
	private InputListener inListener;
	
	private boolean isNewUser = false;
	

	
	public static ClientSkeleton getInstance(){
		if(clientSolution==null){
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}
	
	
	//Create instance and create connection with the provided server info
	@SuppressWarnings("unchecked")
	public ClientSkeleton() {
		
		//If user has specified username but not secret then generate and print
		if(Settings.getUsername() != "anonymous") {
			if(Settings.getSecret() == null) {
				Settings.setSecret(Settings.nextSecret());
				System.out.println("Generating new secret: "+Settings.getSecret());
				isNewUser = true;
			}
		}
		
	    try {
	        createConnection(Settings.getRemoteHostname(), Settings.getRemotePort());
	        System.out.println("Connection with server established");
	        JSONObject msg = new JSONObject();
	        msg.put("username",Settings.getUsername());
	        msg.put("secret", Settings.getSecret());
		    if(isNewUser) {
		    	//Register
		    	msg.put("command","REGISTER");
		    }
		    else {
		    	//Login
		    	msg.put("command", "LOGIN");
		    }
		    outWriter.println(msg);
	    }
	    catch (IOException e) {
	        System.out.println(e);
	    }
		parser = new JSONParser();
		textFrame = new TextFrame();
		inListener = new InputListener();
		start();
	}
	
	
	@SuppressWarnings("unchecked")
	public void sendActivityObject(JSONObject activityObj){
		outWriter.println(activityObj);
		//System.out.println("Message sent");
	}
	
	private void createConnection(String remoteHost, int remotePort) throws UnknownHostException, IOException  {
		serverSocket = new Socket(remoteHost, remotePort);
	    inStream = new DataInputStream(serverSocket.getInputStream());
	    outStream = new DataOutputStream(serverSocket.getOutputStream());
	    inReader = new BufferedReader( new InputStreamReader(inStream));
	    outWriter = new PrintWriter(outStream, true);
	}
	
	public void disconnect(){
		try {
			inReader.close();
			outWriter.close();
		} catch (IOException e) {
			log.error("error closing socket: " + e);
		}
		System.exit(0);
		
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
					System.out.println(data);
					if(json.get("command") == "REDIRECT") {
						reconnect(json);
					}
				} catch (ParseException pe) {
					log.error("error in parsing string :"+pe);
				}
			}
		} catch (IOException e) {
			log.error("connection closed with exception: "+e);
		}
	
	}
	
	
//	Reconnect to other server with provided info, return true if
//	create success
	private boolean reconnect(JSONObject response) {
		Settings.setRemoteHostname(response.get("hostname").toString());
		Settings.setRemotePort((Integer) response.get("port"));
		try {
			createConnection(Settings.getRemoteHostname(), Settings.getRemotePort());
		}
		catch (IOException e) {
			System.out.println("Unable to create socket");	
		}
		return (serverSocket == null);
	}
	
	
	
	//Used to check the integrity of the server response, true if consistent 
	//private boolean checkMessageIntegrity(JSONObject response)

	
}
