package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import activitystreamer.util.Settings;
import org.json.simple.JSONObject;

public class Control extends Thread {

	//Reconnect info
	private static String parentHost = "";
	private static int parentPort = 0;

	private static final Logger log = LogManager.getLogger();

	private static ArrayList<Connection> connections;
	// Client connection
	private static ArrayList<Connection> loadConnections;
	// Server connection
	private static ArrayList<Connection> broadConnections;

	// Database that keeps track of status for each server
	private static HashMap<String, JsonObject> serverInfo = new HashMap<>();

	// Hash maps that record the register user, first one record how many
	// lock allowed is received for it, second one maintain the relation
	// between user name and their connection
	private static HashMap<String, Integer> registerList1 = new HashMap<>();
	private static HashMap<String, Connection> registerList2 = new HashMap<>();

	private static boolean term = false;
	private static Listener listener;

	// Database that record the user login information
	private static HashMap<String, String> userInfo = new HashMap<>();

	// The server id
	private String id = Settings.nextSecret();

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

	private final static String AUTHENTICATE = "AUTHENTICATE";
	private final static String AUTHENTICATION_SUCCESS = "AUTHENTICATION_SUCCESS";
	private final static String AUTHENTICATION_FAIL = "AUTHTENTICATION_FAIL";

	private final static String REDIRECT = "REDIRECT";
	private final static String SERVER_ANNOUNCE = "SERVER_ANNOUNCE";

	protected static Control control = null;

	private JsonParser parser = new JsonParser();

	public static Control getInstance() {
		if (control == null) {
			control = new Control();
		}
		return control;
	}

	public Control() {
		// initialize the connections arrays
		connections = new ArrayList<>();
		loadConnections = new ArrayList<>();
		broadConnections = new ArrayList<>();

		// Generate secret if need
		if (Settings.getSecret() == null) {
			String secret = Settings.nextSecret();
			Settings.setSecret(secret);
			log.info("Using new secret: " + secret);
		}
		// start a listener
		try {
			listener = new Listener();
		} catch (IOException e1) {
			log.fatal("failed to startup a listening thread: " + e1);
			System.exit(-1);
		}

		// Initiate connection
		initiateConnection();
		start();
	}

	public void initiateConnection() {
		// make a connection to another server if remote hostname is supplied
		if (Settings.getRemoteHostname() != null) {
			try {
				outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
			} catch (IOException e) {
				log.error("failed to make connection to " + Settings.getRemoteHostname() + ":"
						+ Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
	}

	/*
	 * Processing incoming messages from the connection. Return true if the
	 * connection should close.
	 */
	public synchronized boolean process(Connection con, String msg) {
		JsonObject receivedMSG;

		try {
			receivedMSG = parser.parse(msg).getAsJsonObject();
			// System.out.println("string to json " + receivedMSG);
		} catch (JsonSyntaxException e) {
			return true;
		}
		try {
			// Check the integrity of the message
			if (!checkMsgIntegrity(con, receivedMSG)) {
				con.closeCon();
				return false;
			}
			String message = receivedMSG.get("command").getAsString();
			switch (message) {
			case REGISTER:
				return !register(con, receivedMSG);
			case LOGIN:
				return !login(con, receivedMSG);
			case AUTHENTICATE:
				return !auth(con, receivedMSG);
			case AUTHENTICATION_SUCCESS:
				return !authSuccess(con, receivedMSG);
			case INVALID_MESSAGE:
				con.closeCon();
				return false;
			case SERVER_ANNOUNCE:
				return !announce(con, receivedMSG);
			case LOGOUT:
				con.closeCon();
				return false;
			case LOCK_REQUEST:
				return !lockRequest(con, receivedMSG);
			case LOCK_DENIED:
				return !lockProcess(con, receivedMSG);
			case LOCK_ALLOWED:
				return !lockProcess(con, receivedMSG);
			case ACTIVITY_BROADCAST:
				return !broadcast(con, receivedMSG);
			case ACTIVITY_MESSAGE:
				return !broadcast(con, receivedMSG);
			case AUTHENTICATION_FAIL:
				con.closeCon();
				return false;
			default:
				return false;
			}
		} catch (NullPointerException e) {
			e.printStackTrace();
			InvalidMessage invalid = new InvalidMessage();
			invalid.setInfo("Not enough info in message," + " possibly no authenticated user in ACTIVITY_BROADCAST ?");
			con.writeMsg(invalid.toJsonString());
		}
		return false;
	}

	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(Connection con) {
		if (!term)
			connections.remove(con);
		if (loadConnections.contains(con))
			loadConnections.remove(con);
		if (broadConnections.contains(con))
			broadConnections.remove(con);
	}

	/*
	 * A new incoming connection has been established, and a reference is returned
	 * to it
	 */
	public synchronized Connection incomingConnection(Socket s) throws IOException {
		log.debug("incomming connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		if (c != null)
			connections.add(c);
		return c;

	}

	/*
	 * A new outgoing connection has been established, and a reference is returned
	 * to it
	 */
	@SuppressWarnings("unchecked")
	public synchronized Connection outgoingConnection(Socket s) throws IOException {
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		Connection c = new Connection(s);
		connections.add(c);
		broadConnections.add(c);
		JSONObject msg = new JSONObject();
		msg.put("command", AUTHENTICATE);
		msg.put("secret", Settings.getSecret());
		c.writeMsg(msg.toJSONString());
		return c;

	}

	/**
	 * Process the register command from client Will first look into local database,
	 * then send lock request to other server.
	 * 
	 * @param con
	 * @param receivedMSG
	 * @return True if register successful, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private synchronized boolean register(Connection con, JsonObject receivedMSG) throws NullPointerException {
		JSONObject regist = new JSONObject();
		String secret = receivedMSG.get("secret").getAsString();
		String username = receivedMSG.get("username").getAsString();
		if (!loadConnections.contains(con)) {
			loadConnections.add(con);
		}
		// If the user is not registered in local database
		if (!userInfo.containsKey(username)) {

			// Try to send lock request if needed
			if (broadConnections.size() > 0) {
				registerList1.put(username, 0);
				registerList2.put(username, con);
				JSONObject registInfo = new JSONObject();
				registInfo.put("command", LOCK_REQUEST);
				registInfo.put("username", username);
				registInfo.put("secret", secret);
				for (Connection server : broadConnections) {
					server.writeMsg(registInfo.toJSONString());
				}
				return true;
			}

			// Otherwise this is a stand alone server, register success
			else {
				userInfo.put(username, secret);
				regist.put("command", REGISTER_SUCCESS);
				regist.put("info", "register successful for " + username);
				con.writeMsg(regist.toJSONString());
				return true;
			}
		}

		// Already registered in database, register fail
		regist.put("command", REGISTER_FAILED);
		regist.put("info", username + " is already register with the system");
		con.writeMsg(regist.toJSONString());
		return false;
	}

	/**
	 * Handles the lock_allowed and lock_denied from other server If received
	 * lock_allowed, increment the counter of the user name by 1 If received
	 * lock_denied, send register fail right away to the user connection
	 * 
	 * @param con
	 * @param receivedMSG
	 * @return True if register successful, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private synchronized boolean lockProcess(Connection con, JsonObject receivedMSG) throws NullPointerException {
		// Check if the message source is authenticated
		if (!broadConnections.contains(con)) {
			InvalidMessage invalidMsg = new InvalidMessage();
			invalidMsg.setInfo("Unanthenticated server");
			con.writeMsg(invalidMsg.toJsonString());
			return false;
		}
		String username = receivedMSG.get("username").getAsString();
		String secret = receivedMSG.get("secret").getAsString();

		// Broadcast message to all other servers except the source
		for (Connection server : broadConnections) {
			if (server != con) {

				// If this is the server that asked for lock response from other server
				// for the coming user name, stop broadcasting
				if (!registerList1.containsKey(username))
					server.writeMsg(receivedMSG.getAsString());
			}
		}

		if (receivedMSG.get("command").getAsString().equals(LOCK_ALLOWED)) {
			if (registerList1.containsKey(username)) {
				int n = registerList1.get(username) + 1;
				registerList1.put(username, n);
				// If the number of allow reaches the number of connected server
				// send register success
				if (n == serverInfo.size()) {
					userInfo.put(username, secret);
					JSONObject response = new JSONObject();
					response.put("command", REGISTER_SUCCESS);
					response.put("info", "register successful for  " + username);
					registerList2.get(username).writeMsg(response.toJSONString());
					registerList1.remove(username);
					registerList2.remove(username);
					return true;
				}
			}

		}
		// If received lock denied, send register failed immediately
		else if (receivedMSG.get("command").getAsString().equals(LOCK_DENIED)) {
			if (registerList1.containsKey(username)) {
				JSONObject response = new JSONObject();
				response.put("command", REGISTER_FAILED);
				response.put("info", username + " is already register with the system");
				registerList2.get(username).writeMsg(response.toJSONString());
				registerList1.remove(username);
				registerList2.remove(username);
				if (userInfo.containsKey(username) && userInfo.get("username").equals(secret)) {
					userInfo.remove(username);
				}
			}
		}
		return true;
	}

	/**
	 * Process the lock request sent from other server Check local database and
	 * return the response to connection
	 * 
	 * @param con
	 * @param receivedMSG
	 * @return True if the message source is authenticated, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private synchronized boolean lockRequest(Connection con, JsonObject receivedMSG) throws NullPointerException {
		if (!broadConnections.contains(con)) {
			InvalidMessage invalidMsg = new InvalidMessage();
			invalidMsg.setInfo("Unanthenticated server");
			con.writeMsg(invalidMsg.toJsonString());
			return false;
		}
		String username = receivedMSG.get("username").getAsString();
		String secret = receivedMSG.get("secret").getAsString();
		JSONObject response = new JSONObject();

		for (Connection server : broadConnections) {
			if (server != con) {
				server.writeMsg(receivedMSG.getAsString());
			}
		}

		// If the user name does not exist in local database
		// store it now
		if (!userInfo.containsKey(username)) {
			response.put("command", LOCK_ALLOWED);
			response.put("username", username);
			response.put("secret", secret);
			userInfo.put(username, secret);
			for (Connection server : broadConnections) {
				server.writeMsg(response.toJSONString());
			}

			return true;
		}
		// Otherwise return lock denied
		response.put("command", LOCK_DENIED);
		response.put("username", username);
		response.put("secret", secret);
		for (Connection server : broadConnections) {
			server.writeMsg(response.toJSONString());
		}
		return true;
	}

	/**
	 * Process the LOGIN command from client
	 * 
	 * @param con
	 *            The connection from which the message is sent
	 * @param receivedMSG
	 *            The coming message
	 * @return True if login successful, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private synchronized boolean login(Connection con, JsonObject receivedMSG) throws NullPointerException {
		JSONObject login = new JSONObject();
		String command;
		String secret = null;
		if (!receivedMSG.get("secret").isJsonNull())
			secret = receivedMSG.get("secret").getAsString();
		String username = receivedMSG.get("username").getAsString();
		int currentLoad = loadConnections.size();

		// If the user login as anonymous or has right name and secret
		// then send login success and check if need to redirect
		if ((username.equals(ANONYMOUS_USERNAME) && secret == null)
				|| (userInfo.containsKey(username) && userInfo.get(username).equals(secret))) {

			command = LOGIN_SUCCESS;
			login.put("command", command);
			login.put("info", "logged in as user " + username);
			if (!loadConnections.contains(con)) {
				loadConnections.add(con);
			}
			con.setUsername(username);
			con.setSecret(secret);
			con.writeMsg(login.toJSONString());

			// Check for redirect
			if (!serverInfo.isEmpty()) {
				JSONObject redirect = new JSONObject();
				redirect.put("command", REDIRECT);
				for (JsonObject info : serverInfo.values()) {
					String hostname = info.get("hostname").toString().replaceAll("\"", "");
					//System.out.print("hostname is " + hostname);
					int load = info.get("load").getAsInt();
					int port = info.get("port").getAsInt();
					if (load + 2 < currentLoad) {
						redirect.put("hostname", hostname);
						redirect.put("port", port);
						System.out.println("redirecting to " + redirect);
						con.writeMsg(redirect.toJSONString());
						con.closeCon();
						return false;
					}
				}
			}
		}
		// Else login fail
		else {
			command = LOGIN_FAILED;
			login.put("command", command);
			login.put("info", "attempt to login with wrong secret");
			con.writeMsg(login.toJSONString());
			return false;
		}

		return true;
	}

	/**
	 * Process the AUTHENTICATE command from other server
	 * 
	 * @param con
	 *            The connection from which the message comes
	 * @param receivedMSG
	 *            The received message
	 * @return True if authenticate success, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private synchronized boolean auth(Connection con, JsonObject receivedMSG) throws NullPointerException {
		JSONObject auth = new JSONObject();
		String command;
		String info;

		// If already authenticated then reply with invalid message
		if (broadConnections.contains(con)) {
			InvalidMessage response = new InvalidMessage();
			response.setInfo("Already authenticated in this server");
			con.writeMsg(response.toJsonString());
			return true;
		}
		// Check if provided secret matches the setting
		String secret = receivedMSG.get("secret").getAsString();
		if (!secret.equals(Settings.getSecret())) {
			command = AUTHENTICATION_FAIL;
			// String authFailMSG = toJsonString();
			info = "the supplied secret is incorrect: " + secret;
			auth.put("command", command);
			auth.put("info", info);
			con.writeMsg(auth.toJSONString());
			return false;
		}

		command = AUTHENTICATION_SUCCESS;
		auth.put("command", command);
		auth.put("myhost", Settings.getLocalHostname());
		auth.put("myport", Settings.getLocalPort());
		auth.put("parenthost", Settings.getRemoteHostname());
		auth.put("parentport", Settings.getRemotePort());
		con.writeMsg(auth.toJSONString());
		broadConnections.add(con);
		return true;

	}

	/**
	 * Process the AUTHENTICATE command from other server
	 *
	 * @param con
	 *            The connection from which the message comes
	 * @param receivedMSG
	 *            The received message
	 * @return True if authenticate success, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private synchronized boolean authSuccess(Connection con, JsonObject receivedMSG) throws NullPointerException {
		JSONObject json = new JSONObject();

		parentHost = receivedMSG.get("parenthost").getAsString();
		parentPort = receivedMSG.get("parentport").getAsInt();

		return true;

	}

	/**
	 * Process the coming server announce message, update local info
	 * 
	 * @param con
	 * @param receivedMSG
	 * @return True if process successfully, false otherwise
	 */
	private synchronized boolean announce(Connection con, JsonObject receivedMSG) throws NullPointerException {
		if (!broadConnections.contains(con)) {
			InvalidMessage invalidMsg = new InvalidMessage();
			invalidMsg.setInfo("Unanthenticated server");
			con.writeMsg(invalidMsg.toJsonString());
			return false;
		}

		// Broadcast announcement to other servers
		for (Connection server : broadConnections) {
			if (con != server) {
				server.writeMsg(receivedMSG.getAsString());
			}
		}
		String hostname = receivedMSG.get("hostname").getAsString();
		serverInfo.put(hostname, receivedMSG);
		return true;
	}

	/**
	 * Process the broadcast message from other servers Update their server info
	 * 
	 * @param con
	 * @param receivedMSG
	 * @return True if the message source is authenticated, false otherwise
	 */
	@SuppressWarnings("unchecked")
	private synchronized boolean broadcast(Connection con, JsonObject receivedMSG) throws NullPointerException {
		if (!loadConnections.contains(con) && !broadConnections.contains(con)) {
			InvalidMessage invalidMsg = new InvalidMessage();
			invalidMsg.setInfo("Unanthenticated connection");
			con.writeMsg(invalidMsg.toJsonString());
			return false;
		}

		// System.out.println("Broadcasting");

		JSONObject response = new JSONObject();
		if (receivedMSG.get("command").getAsString().equals(ACTIVITY_MESSAGE)) {
			String username = receivedMSG.get("username").getAsString();

			String secret = null;
			if (!receivedMSG.get("secret").isJsonNull())
				secret = receivedMSG.get("secret").getAsString();
			if (secret == null || (con.getUsername().equals(username) && con.getSecret().equals(secret))) {
				response.put("command", ACTIVITY_BROADCAST);

				// Process the activity object
				JsonObject actObj = receivedMSG.get("activity").getAsJsonObject();
				actObj.addProperty("authenticated_user", username);
				response.put("activity", actObj);
				for (Connection connection : connections) {
					connection.writeMsg(response.toJSONString());
				}
				return true;
			} else {
				response.put("command", AUTHENTICATION_FAIL);
				response.put("info", "Unauthenticated connection");
				con.writeMsg(response.toJSONString());
				return false;
			}
		} else if (receivedMSG.get("command").getAsString().equals(ACTIVITY_BROADCAST)) {
			response.put("command", ACTIVITY_BROADCAST);
			JsonObject actObj = receivedMSG.get("activity").getAsJsonObject();
			actObj.addProperty("authenticated_user", actObj.get("authenticated_user").getAsString());
			response.put("activity", actObj);
			for (Connection connection : connections) {
				if (connection != con)
					connection.writeMsg(response.toJSONString());
			}
			return true;
		}
		return false;

	}

	@Override
	public void run() {
		//log.info("using activity interval of " + Settings.getActivityInterval() + " milliseconds");
		while (!term) {
			// do something with 5 second intervals in between
			try {
				Thread.sleep(Settings.getActivityInterval());
			} catch (InterruptedException e) {
				log.info("received an interrupt, system is shutting down");
				break;
			}
			if (!term) {
				// log.debug("doing activity");
				term = doActivity();
			}

		}
		log.info("closing " + connections.size() + " connections");
		// clean up
		for (Connection connection : connections) {
			connection.closeCon();
		}
		for (Connection connection : loadConnections) {
			connection.closeCon();
		}
		for (Connection connection : broadConnections) {
			connection.closeCon();
		}
		listener.setTerm(true);
	}

	/**
	 * Broadcast the server states to other server
	 * 
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public synchronized boolean doActivity() {
		JSONObject json = new JSONObject();
		json.put("command", SERVER_ANNOUNCE);
		json.put("id", id);
		json.put("load", loadConnections.size());
		json.put("hostname", Settings.getLocalHostname());
		json.put("port", Settings.getLocalPort());
		for (Connection cons : broadConnections) {
			cons.writeMsg(json.toJSONString());
		}
		return false;
	}

	public final void setTerm(boolean t) {
		term = t;
	}

	public final ArrayList<Connection> getConnections() {
		return connections;
	}

	/**
	 * Checks the integrity of coming message, whether they contain correct field
	 * according to their command
	 * 
	 * @param json
	 *            The coming message
	 * @param con
	 *            The connection from which the message came
	 * @return true if the message is integrate, false otherwise.
	 */
	private boolean checkMsgIntegrity(Connection con, JsonObject json) throws NullPointerException {
		InvalidMessage response = new InvalidMessage();
		if (json.get("command") == null) {
			response.setInfo("No command");
			con.writeMsg(response.toJsonString());
			return false;
		}
		switch (json.get("command").getAsString()) {
		case LOGIN:
			if (json.get("username").isJsonNull()) {
				response.setInfo("Not providing correct username");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case REGISTER:
			if (json.get("username").isJsonNull() || json.get("secret").isJsonNull()) {
				response.setInfo("Not providing correct username or secret");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case AUTHENTICATE:
			if (json.get("secret").isJsonNull()) {
				response.setInfo("Not providing secret");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case ACTIVITY_MESSAGE:
			if (json.get("username").isJsonNull() || json.get("activity").isJsonNull()) {
				response.setInfo("Activity message not complete");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case SERVER_ANNOUNCE:
			if (json.get("hostname").isJsonNull() || json.get("port").isJsonNull() || json.get("load").isJsonNull()
					|| json.get("id").isJsonNull()) {
				response.setInfo("Not providing correct server info");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case ACTIVITY_BROADCAST:
			if (json.get("activity").isJsonNull()) {
				response.setInfo("No activity");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case LOCK_REQUEST:
			if (json.get("username").isJsonNull() || json.get("secret").isJsonNull()) {
				response.setInfo("Not providing username or secret correctly");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case LOCK_ALLOWED:
			if (json.get("username").isJsonNull() || json.get("secret").isJsonNull()) {
				response.setInfo("Not providing username or secret correctly");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case LOCK_DENIED:
			if (json.get("username").isJsonNull() || json.get("secret").isJsonNull()) {
				response.setInfo("Not providing username or secret correctly");
				con.writeMsg(response.toJsonString());
				return false;
			}
			return true;
		case LOGOUT:
			return true;
		case INVALID_MESSAGE:
			con.closeCon();
			return false;
		default:
			response.setInfo("No such command");
			con.writeMsg(response.toJsonString());
			con.closeCon();
			return false;
		}

	}
}
