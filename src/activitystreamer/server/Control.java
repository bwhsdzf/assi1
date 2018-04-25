package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonSyntaxException;
import org.json.*;

import activitystreamer.util.Settings;
import org.json.simple.JSONObject;

public class Control extends Thread {
	private static final Logger log = LogManager.getLogger();

	private static ArrayList<Connection> loadConnections;
	private static ArrayList<Connection> broadConnections;

	private static boolean term=false;
	private static Listener listener;

	private static HashMap<String,String> userInfo = new HashMap<>();

	private final static String INVALID_MESSAGE = "INVALID_MESSAGE";
	private final static String REGISTER = "REGISTER";
	private final static String REGISTER_SUCCESS = "REGISTER_SUCCESS";
	private final static String REGISTER_FAILED = "REGISTER_FAILED";
	private final static String ANONYMOUS_USERNAME = "ANONYMOUS_USERNAME";

	private final static String LOGIN = "LOGIN";
	private final static String LOGIN_SUCCESS = "LOGIN_SUCCESS";
	private final static String LOGIN_FAILED = "LOGIN_FAILED";
	private final static String LOGOUT = "LOGOUT";

	private final static String CLIENT_AUTHENTICATE = "CLIENT_AUTHENTICATE";
	private final static String ACTIVITY_BROADCAST = "ACTIVITY_BROADCAST";
	private final static String ACTIVITY_MESSAGE = "ACTIVITY_MESSAGE";

	private final static String AUTHENTICATION_FAIL = "AUTHTENTICATION_FAIL";
	private final static String AUTHENTICATE = "AUTHENTICATE";
	private final static String REPEATED_AUTHENTICATION = "REPEATED_AUTHENTICATION";
	private final static String UNAUTHENTICATED_SERVER = "UNAUTHENTICATED_SERVER";

	private final static String REDIRECT = "REDIRECT";
	private final static String SERVER_ANNOUNCE = "SERVER_ANNOUNCE";
	
	protected static Control control = null;
	
	//static 这个类的类对象/类方法，通过类名调用，包含了这个类的方法。如果不是类方法，需要类对象调用，listener要调用的话得通过静态方法。这样不管怎么调用都是同一个control就是
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
		String secret = Settings.nextSecret();
		Settings.setSecret(secret);
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
	 * synchronized 同时只能被一个线程使用
	 * 出现了问题 term=true 下一个循环就关闭connection
	 * regist
	 * login
	 * logout
	 * active
	 */
	public synchronized boolean process(Connection con,String msg){
		JsonObject receivedMSG;
		String command;
		String info;
		try
		{
			receivedMSG = new Gson().fromJson(msg, JsonObject.class);
		}
		catch (JsonSyntaxException e)
		{
			return true;
		}
		if (!receivedMSG.has("command"))
		{
			InvalidMessage invalidMessage = new InvalidMessage();
			invalidMessage.setInfo("the received message did not contain a command");
			con.writeMsg(invalidMessage.toJsonString());
			return true;
		}
		String message = receivedMSG.get("command").getAsString();
		switch(message){
			case REGISTER:
				return register(con, receivedMSG);
			case LOGIN:
				return login(con, receivedMSG);
			case AUTHENTICATE:
				return auth(con, receivedMSG);
			case INVALID_MESSAGE:
				return invalid(con, receivedMSG);
			case SERVER_ANNOUNCE:
				return announce(con, receivedMSG);
			case LOGOUT:
				return logout(con, receivedMSG);
			default:
				return processInvalidCommand(con, receivedMSG);
		}
	}
	
	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con){
		if(!term) connections.remove(con);
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
		loadConnections.add(c);
		return c;
		
	}

	private boolean validUserInfo(Connection con, JsonObject receivedMSG) {
		InvalidMessage invalidMsg = new InvalidMessage();
		if (!receivedMSG.has("username")) {
			invalidMsg.setInfo("username must be contained");
			con.writeMsg(invalidMsg.toJsonString());
			return false;
		} else if (!receivedMSG.has("secret")) {
			invalidMsg.setInfo("secret must be contained");
			con.writeMsg(invalidMsg.toJsonString());
			return false;
		} else {
			return true;
		}
	}

	//封装JSON有点问题。。不知道command和info怎么弄到一起去然后现在这个JSONObject大概要重写一个write= =
	private boolean register(Connection con, JsonObject receivedMSG){
		String command;
		String info;
		JSONObject regist = new JSONObject();
		if(!validUserInfo(con, receivedMSG)){
			return true;
		}
		String secret = receivedMSG.get("secret").getAsString();
		String username = receivedMSG.get("username").getAsString();
		if (userInfo.containsKey(username) || username.equals(ANONYMOUS_USERNAME)){
			command = REGISTER_FAILED;
			//String registFailedMSG = toJsonString(command);
			info = username + " is already registered with the system.";
			//String registFailedInfo = toJsonString(info);
			regist.put(command,info);
			con.writeMsg(regist);
			return true;
		}else{
			command = REGISTER_SUCCESS;
			//String registSuccessMSG = toJsonString(command);
			info = "register success for " + username;
			//String registSuccessInfo = toJsonString(info);
			regist.put(command,info);
			con.writeMsg(regist);
			userInfo.put(username,secret);
			return true;
		}
	}

	private boolean login(Connection con, JsonObject receivedMSG){
		JSONObject login = new JSONObject();
		String command;
		String info;
		if(!validUserInfo(con, receivedMSG)){
			return true;
		}
		String secret = receivedMSG.get("secret").getAsString();
		String username = receivedMSG.get("username").getAsString();
		if(!username.equals(ANONYMOUS_USERNAME) && !(userInfo.containsKey(username)
				&& userInfo.get(username).equals(secret))){
			command = LOGIN_FAILED;
			//String loginFailedMSG = toJsonString(command);
			info = "attempt to login with wrong secret";
			//String loginFailedInfo = toJsonString(info);
			login.put(command,info);
			con.writeMsg(login);
			return true;
		}
		//这里应该是登录成功然后redirect，缺一段找其他server的代码
		else {
			command = LOGIN_SUCCESS;
			//String loginSuccessMSG = toJsonString(command);
			info = "logged in as user " + username;
			//String loginSucessInfo = toJsonString()
			login.put(command,info);
			con.writeMsg(login);
			//if有空 else都很忙
		}
		return true;
	}

	private boolean auth(Connection con, JsonObject receivedMSG){
		JSONObject auth = new JSONObject();
		String command;
		String info;
		if(!receivedMSG.has("secret")){
			command = AUTHENTICATION_FAIL;
			//String authFailMSG = toJsonString();
			info = "the supplied secret is incorrect";
			auth.put(command,info);
			con.writeMsg(auth);
			return  true;
		}
		String secret = receivedMSG.get("secret").getAsString();
		if(!secret.equals(Settings.getSecret())){
			command = AUTHENTICATION_FAIL;
			//String authFailMSG = toJsonString();
			info = "the supplied secret is incorrect: " + secret;
			auth.put(command,info);
			con.writeMsg(auth);
			return  true;
		}
		for (Connection connection : loadConnections) {
			if (con.getSocket().getPort() == connection.getSocket().getPort()) {
				InvalidMessage invalidMsg = new InvalidMessage();
				invalidMsg.setInfo(REPEATED_AUTHENTICATION);
				con.writeMsg(invalidMsg.toJsonString());
				return true;
			}else{
				loadConnections.add(con);
				return false;
			}
		}
		return false;
	}

	private boolean invalid(Connection con, JsonObject receivedMSG){
		String errorInfo = receivedMSG.get("info").getAsString();
		if (errorInfo.equals(UNAUTHENTICATED_SERVER) ||
				errorInfo.equals(REPEATED_AUTHENTICATION))
		{
			return true;
		}
		return false;
	}

	private boolean logout(Connection con, JsonObject receivedMSG){
		String logoutInfo = receivedMSG.get("command").getAsString();
		if(logoutInfo.equals(LOGOUT)){
			return true;
		}
		return false;
	}


	//这个地方是broadcast了
	private boolean announce(Connection con, JsonObject receivedMSG){
		if(!loadConnections.contains(con)){
			InvalidMessage invalidMsg = new InvalidMessage();
			invalidMsg.setInfo(UNAUTHENTICATED_SERVER);
			con.writeMsg(invalidMsg.toJsonString());
			return true;
		}


	}

	private boolean processInvalidCommand(Connection con, JsonObject receivedMSG){
		String command = receivedMSG.get("command").getAsString();
		InvalidMessage invalidMsg = new InvalidMessage();
		invalidMsg.setInfo("Invalid command: " + command);
		con.writeMsg(invalidMsg.toJsonString());
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
		listener.setTerm(true);
	}
	
	public boolean doActivity(){
		return false;
	}
	
	public final void setTerm(boolean t){
		term=t;
	}
	
	public final ArrayList<Connection> getConnections() {
		return connections;
	}
}
