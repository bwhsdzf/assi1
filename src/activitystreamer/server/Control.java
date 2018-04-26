package activitystreamer.server;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;

import activitystreamer.util.Settings;
import org.json.simple.JSONObject;

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();
	
	private static ArrayList<Connection> connections;
	//Client connection
	private static ArrayList<Connection> loadConnections;
	//Server connection
	private static ArrayList<Connection> broadConnections;
	
	
	//Database that keeps track of stats for each server
	private static HashMap<String,JsonObject> serverInfo = new HashMap<>();

	private static boolean term=false;
	private static Listener listener;

	private static HashMap<String,String> userInfo = new HashMap<>();

	private final static String INVALID_MESSAGE = "INVALID_MESSAGE";
	private final static String REGISTER = "REGISTER";
	private final static String REGISTER_SUCCESS = "REGISTER_SUCCESS";
	private final static String REGISTER_FAILED = "REGISTER_FAILED";
	private final static String ANONYMOUS_USERNAME = "anonymous";

	private final static String LOGIN = "LOGIN";
	private final static String LOGIN_SUCCESS = "LOGIN_SUCCESS";
	private final static String LOGIN_FAILED = "LOGIN_FAILED";
	private final static String LOGOUT = "LOGOUT";
	
	private final static String LOCK_REQUEST = "LOCK_REQUEST";
	private final static String LOCK_DENIED = "LOCK_DENIED";
	private final static String LOCK_ALLOWED = "LOCK_ALLOWED";


	private final static String ACTIVITY_BROADCAST = "ACTIVITY_BROADCAST";
	private final static String ACTIVITY_MESSAGE = "ACTIVITY_MESSAGE";

	private final static String AUTHENTICATION_FAIL = "AUTHTENTICATION_FAIL";
	private final static String AUTHENTICATE = "AUTHENTICATE";

	private final static String REDIRECT = "REDIRECT";
	private final static String SERVER_ANNOUNCE = "SERVER_ANNOUNCE";
	
	protected static Control control = null;
	
	public static Control getInstance() {
		if(control==null){
			control=new Control();
		} 
		return control;
	}
	
	public Control() {
		// initialize the connections array
		loadConnections = new ArrayList<>();
		broadConnections = new ArrayList<>();
		if(Settings.getSecret() == null) {
			String secret = Settings.nextSecret();
			Settings.setSecret(secret);
			System.out.println("Using new secret: "+ secret);
		}
		// start a listener
		try {
			listener = new Listener();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: "+e1);
			System.exit(-1);
		}	
	}
	
	public void initiateConnection(){
		// make a connection to another server if remote hostname is supplied
		if(Settings.getRemoteHostname()!=null){
			try {
				outgoingConnection(new Socket(Settings.getRemoteHostname(),Settings.getRemotePort()));
			} catch (IOException e) {
				log.error("failed to make connection to "+Settings.getRemoteHostname()+":"+Settings.getRemotePort()+" :"+e);
				System.exit(-1);
			}
		}
	}


	/*
	 * Processing incoming messages from the connection.
	 * Return true if the connection should close.
	 */
	public synchronized boolean process(Connection con, String msg){
		JsonObject receivedMSG;
		try
		{
			receivedMSG = new Gson().fromJson(msg, JsonObject.class);
		}
		catch (JsonSyntaxException e)
		{
			return true;
		}
		if(!checkMsgIntegrity(con, receivedMSG)) {
			return false;
		}
		String message = receivedMSG.get("command").getAsString();
		switch(message){
			case REGISTER:
				return !register(con, receivedMSG);
			case LOGIN:
				return !login(con, receivedMSG);
			case AUTHENTICATE:
				return !auth(con, receivedMSG);
			case INVALID_MESSAGE:
				return true;
			case SERVER_ANNOUNCE:
				return !announce(con, receivedMSG);
			case LOGOUT:
				return true;
			case LOCK_REQUEST:
				return !lockRequest(con,receivedMSG);
			case LOCK_DENIED:
				return false;
				//return lockDenied(con,receivedMSG);
			default:
				return false;
		}
	}
	
	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con){
		if(!term) connections.remove(con);
		if(loadConnections.contains(con)) loadConnections.remove(con);
		if(broadConnections.contains(con)) broadConnections.remove(con);
	}
	
	/*
	 * A new incoming connection has been established, and a reference is returned to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException{
		log.debug("incomming connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}
	
	/*
	 * A new outgoing connection has been established, and a reference is returned to it
	 */
	public synchronized Connection outgoingConnection(Socket s) throws IOException{
		log.debug("outgoing connection: "+Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		return c;
		
	}


	/**Process the register command from client
	 * Will first look into local database, then send lock request to
	 * other server.
	 * @param con
	 * @param receivedMSG
	 * @return True if register successful, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private boolean register(Connection con, JsonObject receivedMSG){
		JSONObject regist = new JSONObject();
		String secret = receivedMSG.get("secret").getAsString();
		String username = receivedMSG.get("username").getAsString();
		if(!loadConnections.contains(con)) {
			loadConnections.add(con);
		}
		
		//Still need to send lock request etc.
		//TO_DO(*^*%^*&%&$*&*&
		if(!userInfo.containsKey(username)) {
			userInfo.put(username, secret);
			regist.put("command", REGISTER_SUCCESS);
			regist.put("info", "register successful for" + username);
			con.writeMsg(regist.toJSONString());
			return true;
		}
		else {
			regist.put("command", REGISTER_FAILED);
			regist.put("info", username+ " is already register with the system");
			con.writeMsg(regist.toJSONString());
		}
		
		
		
		return true;
	}
	
	
	/**Process the lock request sent from other server
	 * 
	 * @param con
	 * @param receivedMSG
	 * @return Always true to indicated the connection should not be closed
	 */
	@SuppressWarnings("unchecked")
	private boolean lockRequest(Connection con, JsonObject receivedMSG) {
		String username = receivedMSG.get("username").getAsString();
		String secret = receivedMSG.get("secret").getAsString();
		JSONObject response = new JSONObject();
		if(!userInfo.containsKey(username)) {
			response.put("command", LOCK_ALLOWED);
			response.put("username", username);
			response.put("secret", secret);
			userInfo.put(username, secret);
		}
		response.put("command", LOCK_DENIED);
		response.put("username", username);
		response.put("secret", secret);
		con.writeMsg(response.toJSONString());
		return true;
	}

	
	/**Process the LOGIN command from client
	 * 
	 * @param con The connection from which the message is sent
	 * @param receivedMSG The coming message
	 * @return True if login successful, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private boolean login(Connection con, JsonObject receivedMSG){
		JSONObject login = new JSONObject();
		String command;
		String secret = receivedMSG.get("secret").getAsString();
		String username = receivedMSG.get("username").getAsString();
		int currentLoad = loadConnections.size();
	
		
		//If the user login as anoneymous or has right name and secret
		//then send login success and check if need to redirect
		if( (username.equals(ANONYMOUS_USERNAME) && secret == null) ||
				userInfo.get(username).equals(secret)){
			command = LOGIN_SUCCESS;
			login.put("command",command);
			login.put("info","logged in as user " + username);
			
			for(JsonObject info:serverInfo.values()){
				String hostname = info.get("hostname").toString();
				int load =info.get("load").getAsInt();
				int port =info.get("port").getAsInt();
				if(load+2<currentLoad){
					login.put("command",REDIRECT);
					login.put("hostname",hostname);
					login.put("port",port);
					return false;
				}
			}
		}
		//Else login fail
		else{
			command = LOGIN_FAILED;
			login.put("command",command);
			login.put("info", "attempt to login with wrong secret");
			con.writeMsg(login.toJSONString());
			return true;
		}
		
		return true;
	}

	
	/**Process the AUTHENTICATE command from other server
	 * @param con  The connection from which the message comes
	 * @param receivedMSG The received message
	 * @return True if authenticate success, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private boolean auth(Connection con, JsonObject receivedMSG){
		JSONObject auth = new JSONObject();
		String command;
		String info;
		
		//If already authenticated then reply with invalid message
		if(broadConnections.contains(con)) {
			InvalidMessage response = new InvalidMessage();
			response.setInfo("Already authenticated in this server");
			con.writeMsg(response.toJsonString());
			return true;
		}
		//Check if provided secret matches the setting
		String secret = receivedMSG.get("secret").getAsString();
		if(!secret.equals(Settings.getSecret())){
			command = AUTHENTICATION_FAIL;
			//String authFailMSG = toJsonString();
			info = "the supplied secret is incorrect: " + secret;
			auth.put("command",command);
			auth.put("info",info);
			con.writeMsg(auth.toJSONString());
			return false;
		}
		broadConnections.add(con);
		return true;
	}
	
	/**Process the coming server announce message, update local info
	 * 
	 * @param con
	 * @param receivedMSG
	 * @return True if process successfully, false otherwise
	 */
	private boolean announce(Connection con, JsonObject receivedMSG){
		if(!broadConnections.contains(con)){
			InvalidMessage invalidMsg = new InvalidMessage();
			invalidMsg.setInfo("Unanthenticated server");
			con.writeMsg(invalidMsg.toJsonString());
			return false;
		}
		String hostname = receivedMSG.get("hostname").getAsString();
		serverInfo.put(hostname, receivedMSG);
		return true;
	}
	
	@Override
	public void run(){
		log.info("using activity interval of "+Settings.getActivityInterval()+" milliseconds");
		while(!term){
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}
			if(!term){
				log.debug("doing activity");
				term=doActivity();
			}
			
		}
		log.info("closing "+connections.size()+" connections");
		// clean up
		for(Connection connection : connections){
			connection.closeCon();
		}
		for(Connection connection : loadConnections){
			connection.closeCon();
		}
		for(Connection connection : broadConnections){
			connection.closeCon();
		}
		listener.setTerm(true);
	}
	
	
	//Broadcasting message to other server
	@SuppressWarnings("unchecked")
	public boolean doActivity(){
		try {
			String localHost = InetAddress.getLocalHost().getHostName();
			System.out.println("local host name is "+ localHost);
			String id = Settings.nextSecret().substring(0, 5);
			JSONObject json = new JSONObject();
			json.put("command", SERVER_ANNOUNCE);
			json.put(id,Settings.getSecret());
			json.put("load", loadConnections.size());
			json.put("hostname", Settings.getLocalHostname());
			json.put("port", Settings.getLocalPort());
			for(Connection cons:broadConnections) {
				cons.writeMsg(json.toJSONString());
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		return false;
	}
	
	public final void setTerm(boolean t){
		term=t;
	}
	
	public final ArrayList<Connection> getConnections() {
		return connections;
	}
	
	
	/**Checks the integrity of coming message, whether they contain correct field according to
	 * their command
	 * @param json The coming message
	 * @param con The connection from which the message came
	 * @return true if the message is integrate, false otherwise.
	 */
	private boolean checkMsgIntegrity(Connection con,JsonObject json) {
		InvalidMessage response = new InvalidMessage();
		if(json.get("command")!= null) {
			response.setInfo("No command");
			return false;
		}
		switch (json.get("command").toString()) {
		case LOGIN:
			if(json.get("username")!= null && json.get("secret")!= null) {
				response.setInfo("Not providing correct username or secret");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case REGISTER:
			if(json.get("username")!= null && json.get("secret")!= null) {
				response.setInfo("Not providing correct username or secret");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case AUTHENTICATE:
			if(json.get("secret")==null) {
				response.setInfo("Not providing secret");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case ACTIVITY_MESSAGE:
			if(json.get("username")!= null && json.get("secret")!= null) {
				response.setInfo("Not providing correct username or secret");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case SERVER_ANNOUNCE:
			if(json.get("hostname")!= null && json.get("port")!= null && json.get("load")!= null) {
				response.setInfo("Not providing correct server info");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case ACTIVITY_BROADCAST:
			if(json.get("activity")!= null) {
				response.setInfo("No activity");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case LOCK_REQUEST:
			if(json.get("username")!= null && json.get("secret")!= null) {
				response.setInfo("Not providing username or secret correctly");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case LOCK_ALLOWED:
			if(json.get("username")!= null && json.get("secret")!= null) {
				response.setInfo("Not providing username or secret correctly");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case LOCK_DENIED:
			if(json.get("username")!= null && json.get("secret")!= null) {
				response.setInfo("Not providing username or secret correctly");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		default:
			response.setInfo("No such command");
			con.writeMsg(response.toJsonString());
			return false;
		}
		
	}
}
