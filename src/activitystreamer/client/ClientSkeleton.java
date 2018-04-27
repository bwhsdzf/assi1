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
	
	private final static String LOGIN = "LOGIN";
	private final static String LOGIN_SUCCESS = "LOGIN_SUCCESS";
	private final static String LOGIN_FAILED = "LOGIN_FAILED";
	private final static String LOGOUT = "LOGOUT";
	private final static String ACTIVITY_BROADCAST = "ACTIVITY_BROADCAST";
	private final static String ACTIVITY_MESSAGE = "ACTIVITY_MESSAGE";

	private final static String AUTHENTICATION_FAIL = "AUTHTENTICATION_FAIL";
	private final static String REDIRECT = "REDIRECT";
	private final static String INVALID_MESSAGE = "INVALID_MESSAGE";
	private final static String REGISTER = "REGISTER";
	private final static String REGISTER_SUCCESS = "REGISTER_SUCCESS";
	private final static String REGISTER_FAILED = "REGISTER_FAILED";

	public static ClientSkeleton getInstance() {
		if (clientSolution == null) {
			clientSolution = new ClientSkeleton();
		}
		return clientSolution;
	}

	// Create instance and create connection with the provided server info
	@SuppressWarnings("unchecked")
	public ClientSkeleton() {

		// If user has specified username but not secret then generate and print
		if (Settings.getUsername() != "anonymous") {
			if (Settings.getSecret() == null) {
				Settings.setSecret(Settings.nextSecret());
				System.out.println("Generating new secret: " + Settings.getSecret());
				isNewUser = true;
			}
		}

		try {
			createConnection(Settings.getRemoteHostname(), Settings.getRemotePort());
			System.out.println("Connection with server established");
			JSONObject msg = new JSONObject();
			msg.put("username", Settings.getUsername());
			msg.put("secret", Settings.getSecret());
			if (isNewUser) {
				// Register
				msg.put("command", REGISTER);
			} else {
				// Login
				msg.put("command", LOGIN);
			}
		 // System.out.println(msg);
			outWriter.println(msg);
		} catch (IOException e) {
			System.out.println(e);
		}
		parser = new JSONParser();
		textFrame = new TextFrame();
		inListener = new InputListener();
		start();
	}

	@SuppressWarnings("unchecked")
	public void sendActivityObject(JSONObject activityObj) {
		JSONObject msg = new JSONObject();
		msg.put("command", ACTIVITY_MESSAGE);
		msg.put("username", Settings.getUsername());
		msg.put("secret", Settings.getSecret());
		msg.put("activity", activityObj);
		// System.out.println(msg);
		outWriter.println(msg.toJSONString());
	}

	private void createConnection(String remoteHost, int remotePort) throws UnknownHostException, IOException {
		serverSocket = new Socket(remoteHost, remotePort);
		inStream = new DataInputStream(serverSocket.getInputStream());
		outStream = new DataOutputStream(serverSocket.getOutputStream());
		inReader = new BufferedReader(new InputStreamReader(inStream));
		outWriter = new PrintWriter(outStream, true);
	}

	@SuppressWarnings("unchecked")
	public void disconnect() {
		JSONObject msg = new JSONObject();
		msg.put("command", LOGOUT);
		outWriter.println(msg.toJSONString());
		try {
			inReader.close();
			outWriter.close();
		} catch (IOException e) {
			log.error("error closing socket: " + e);
		}

	}

	// Process all incoming message from server
	public void run() {
		// System.out.println(serverSocket.getInetAddress());
		try {
			JSONObject json;
			String data;
			while ((data = inReader.readLine()) != null) {
				try {
					json = (JSONObject) parser.parse(data);
					System.out.println(data);
					if (checkMessageIntegrity(json)) {
						switch (json.get("command").toString()) {
						case ACTIVITY_BROADCAST:
							textFrame.setOutputText(json);
							continue;
						case REDIRECT:
							if(!reconnect(json))
								System.exit(-1);
							else continue;
						case LOGIN_FAILED:
							System.exit(-1);
						case REGISTER_FAILED:
							System.exit(-1);
						case INVALID_MESSAGE:
							System.exit(-1);
						default:
							//Do nothing
							continue;
						}
					}
				} catch (ParseException pe) {
					log.error("error in parsing string :" + pe);
				}
			}
		} catch (IOException e) {
			log.error("connection closed with exception: " + e);
		}
		System.exit(0);

	}

	// Reconnect to other server with provided info, return true if
	// create success
	@SuppressWarnings("unchecked")
	private boolean reconnect(JSONObject response) {
		Settings.setRemoteHostname(response.get("hostname").toString());
		
		
		Settings.setRemotePort(Integer.parseInt(response.get("port").toString()));
		try {
			createConnection(Settings.getRemoteHostname(), Settings.getRemotePort());
			JSONObject msg = new JSONObject();
			msg.put("command", LOGIN);
			msg.put("username", Settings.getUsername());
			msg.put("secret", Settings.getSecret());
			outWriter.println(msg.toJSONString());
		} catch (IOException e) {
			System.out.println("Unable to create socket");
		}
		return (serverSocket != null);
	}

	// Used to check the integrity of the server response, true if consistent
	@SuppressWarnings("unchecked")
	private boolean checkMessageIntegrity(JSONObject response) {
		if (response.get("command") != null) {
			JSONObject invalidMsg = new JSONObject();
			invalidMsg.put("command", INVALID_MESSAGE);
			switch (response.get("command").toString()) {
			case REDIRECT:
				if (response.get("hostname") != null && response.get("port") != null) {
					return true;
				} else {
					System.out.println("Not enough info to redirect");
					System.exit(-1);
				}

			case LOGIN_SUCCESS:
				if (response.get("info") != null) {
					return true;
				} else {
					invalidMsg.put("info", "No info provided");
					outWriter.println(invalidMsg);
					System.exit(-1);
				}

			case LOGIN_FAILED:
				if (response.get("info") != null) {
					return true;
				} else {
					System.out.println("Error, no info in message");
					System.exit(-1);
				}

			case ACTIVITY_BROADCAST:
				if (response.get("activity") != null) {
					return true;
				} else {
					invalidMsg.put("info", "No activity message");
					outWriter.println(invalidMsg);
					System.exit(-1);
				}

			case REGISTER_SUCCESS:
				if (response.get("info") != null) {
					JSONObject login = new JSONObject();
					login.put("command", "LOGIN");
					login.put("username", Settings.getUsername());
					login.put("secret", Settings.getSecret());
					outWriter.println(login);
					return true;
				} else {
					invalidMsg.put("info", "No info provided");
					outWriter.println(invalidMsg);
					System.exit(-1);
				}

			case REGISTER_FAILED:
				if (response.get("info") != null) {
					return true;
				} else {
					System.out.println("Error, no info in message");
					System.exit(-1);
				}
			case AUTHENTICATION_FAIL:
				if (response.get("info") != null) {
					return true;
				} else {
					System.out.println("Error, no info in message");
					System.exit(-1);
				}
			default:
				invalidMsg.put("info", "No  valid command");
				outWriter.println(invalidMsg);
				
			}
		}
		return false;
	}
}
