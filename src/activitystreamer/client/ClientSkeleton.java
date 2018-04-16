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
	
//	private Socket serverSocket
//	private DataOutputStream outStream;
//	private DataInputStream inStream;
//	private BufferedReader inReader;
//	private PrintWriter outWriter;
	

	
	public static ClientSkeleton getInstance(){
		if(clientSolution==null){
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}
	
	
	//Create instance and create connection with the provided server info
	public ClientSkeleton(){
		
		
		textFrame = new TextFrame();
		start();
	}
	
	
	
	
	
	
	@SuppressWarnings("unchecked")
	public void sendActivityObject(JSONObject activityObj){
		
	}
	
	
	public void disconnect(){
		
	}
	
	
	public void run(){

	}
	
	
	//Reconnect to other server with provided info, return true if
	//create success
	//private boolean reconnect(JSONObject response)
	
	
	
	//Used to check the integrity of the server response, true if consistent 
	//private boolean checkMessageIntegrity(JSONObject response)

	
}
