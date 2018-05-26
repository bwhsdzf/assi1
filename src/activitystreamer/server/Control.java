package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.*;
import java.sql.Timestamp;


import activitystreamer.Connector.*;
import activitystreamer.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import activitystreamer.util.*;

public class Control extends Thread {

	private static final Logger log = LogManager.getLogger();

	private static ServerConnector outGoingConnection;
	private static ArrayList<ServerConnector> serverInConnections;
	private static ArrayList<ClientConnector> clientConnections;

	private static ArrayList<ServerConnector> connections;
	// Client connection
	private static ArrayList<ServerConnector> loadConnections;
	// Server connection
	private static ArrayList<ServerConnector> broadConnections;

	// Database that keeps track of status for each server
	private static HashMap<String, JsonObject> serverInfo = new HashMap<>();

	// Hash maps that record the register user, first one record how many
	// lock allowed is received for it, second one maintain the relation
	// between user name and their connection
	private static HashMap<String, Integer> registerList1 = new HashMap<>();
	private static HashMap<String, ServerConnector> registerList2 = new HashMap<>();

	private static HashMap<String, Integer> loginList1 = new HashMap<>();
	private static HashMap<String, ServerConnector> loginList2 = new HashMap<>();

	private static HashMap<String, ArrayList<Message>> messageLog = new HashMap<>();

	private static boolean term = false;
	private static Listener listener;

	private Timestamp disconnectTime;

	// Database that record the user login information
	private static HashMap<String, String> userInfo = new HashMap<>();

	// The server id
	private String id = Settings.nextSecret();

	protected static Control control = null;

	private JsonParser parser = new JsonParser();

	// Finalized
	public static Control getInstance() {
		if (control == null) {
			control = new Control();
		}
		return control;
	}

	public void setDisconectTime(Timestamp time) {
		this.disconnectTime = time;
	}

	public Control() {
		// NEW CONNECTION STORAGE
		outGoingConnection = null;
		serverInConnections = new ArrayList<>();
		clientConnections = new ArrayList<>();

		//NEED REMOVING FINALLY
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

	// Finalized
	public void initiateConnection() {
		// make a connection to another server if remote hostname is supplied
		if (Settings.getRemoteHostname() != null) {
			try {
				outgoingConnection(new Socket(Settings.getRemoteHostname(), Settings.getRemotePort()));
			} catch (IOException e) {
				log.error("Failed to make connection to " + Settings.getRemoteHostname() + ":"
						+ Settings.getRemotePort() + " :" + e);
				System.exit(-1);
			}
		}
	}

	/*
	 * Processing incoming messages from the connection. Return true if the
	 * connection should close.
	 */
	public synchronized boolean process(ServerConnector con, String msg) {
		JsonObject receivedMSG;

		try {
			receivedMSG = parser.parse(msg).getAsJsonObject();
			// System.out.println("string to json " + receivedMSG);
		} catch (JsonSyntaxException e) {
			return true;
		}
		try {
			String type = receivedMSG.get("command").getAsString();

			if (type.equals(Protocol.Type.REGISTER.name())) {
				System.out.println(type);
				return !register(con, receivedMSG);
			} else if (type.equals(Protocol.Type.LOGIN.name())) {
				System.out.println(type);
				return !login(con, receivedMSG);
			} else if (type.equals(Protocol.Type.AUTHENTICATE.name())) {
				System.out.println(type);
				return !auth(con, receivedMSG);
			} else if (type.equals(Protocol.Type.AUTHENTICATION_SUCCESS.name())) {
				System.out.println(type);
				return !authSuccess(con, receivedMSG);
			} else if (type.equals(Protocol.Type.INVALID_MESSAGE.name())) {
				System.out.println(type);
				con.closeCon();
				return false;
			} else if (type.equals(Protocol.Type.SERVER_ANNOUNCE.name())) {
				System.out.println(type);
				return !announce(con, receivedMSG);
			} else if (type.equals(Protocol.Type.LOGOUT.name())) {
				System.out.println(type);
				con.closeCon();
				return false;
			} else if (type.equals(Protocol.Type.LOCK_REQUEST.name())) {
				System.out.println(type);
				return !lockRequest(con, receivedMSG);
			} else if (type.equals(Protocol.Type.LOCK_DENIED.name())) {
				System.out.println(type);
				return !lockProcess(con, receivedMSG);
			} else if (type.equals(Protocol.Type.LOCK_ALLOWED.name())) {
				System.out.println(type);
				return !lockProcess(con, receivedMSG);
			} else if (type.equals(Protocol.Type.ACTIVITY_BROADCAST.name())) {
				System.out.println(type);
				return !broadcast(con, receivedMSG);
			} else if (type.equals(Protocol.Type.ACTIVITY_MESSAGE.name())) {
				System.out.println(type);
				return !receiveMessage(con, receivedMSG);
			} else if (type.equals(Protocol.Type.AUTHTENTICATION_FAIL.name())) {
				System.out.println(type);
				con.closeCon();
				return false;
			} else if (type.equals(Protocol.Type.LOGIN_REQUEST.name())) {
				return !loginRequest(con, receivedMSG);
			} else if (type.equals(Protocol.Type.LOGIN_ALLOWED.name())) {
				return !loginProcess(con, receivedMSG);
			} else if (type.equals(Protocol.Type.LOGIN_DENIED.name())) {
				return !loginProcess(con, receivedMSG);
			}else if (type.equals(Protocol.Type.SEND_ALL_MESSAGE.name())) {
				return !sendAllMessage(con, receivedMSG);
			}else if(type.equals(Protocol.Type.UPDATE_BACKUP.name())){
				System.out.println(type);
				return !updateBackup(con,receivedMSG);
			}else
				return false;
		} catch (NullPointerException e) {
			e.printStackTrace();
			msg = Protocol.invalidMessage(
					"Not enough info in message," + " possibly no authenticated user in ACTIVITY_BROADCAST ?");
			con.writeMsg(msg);
		}
		return false;
	}

	/*
	 * The connection has been closed by the other party.
	 */
	public synchronized void connectionClosed(ServerConnector con) {
		if (!term)
			connections.remove(con);
		if (loadConnections.contains(con))
			loadConnections.remove(con);
		if (broadConnections.contains(con))
			broadConnections.remove(con);
	}

	public synchronized ServerConnector reconnect(ServerConnector con) {
		try {
			if (!term)
				connections.remove(con);
			if (loadConnections.contains(con))
				loadConnections.remove(con);
			if (broadConnections.contains(con))
				broadConnections.remove(con);

			Socket s = new Socket(Settings.getBackupHostname(), Settings.getBackupHostPort());
			/*ServerSocket s = new ServerSocket(Settings.getBackupHostname(), Settings.getBackupHostPort());*/
			ServerConnector c = new ServerConnector(s, true);

			connections.add(c);
			broadConnections.add(c);
			String msg = Protocol.authenticate(Settings.getSecret(), true, disconnectTime.getTime());
			c.writeMsg(msg);

			log.debug("This message should appear after get new backup");
			return c;
			

		} catch (IOException e) {
			log.error("failed to make RE-connection to " + Settings.getRemoteHostname() + ":" + Settings.getRemotePort()
					+ " :" + e);
			System.exit(-1);
		}
		return null;

	}

	/*
	 * A new incoming connection has been established, and a reference is returned
	 * to it
	 */
	public synchronized ServerConnector incomingConnection(Socket s) throws IOException {
		log.debug("incomming connection: " + Settings.socketAddress(s));
		ServerConnector c = new ServerConnector(s, false);


		if (c != null)
			connections.add(c);
		return c;
		/*log.debug("incomming connection: " + Settings.socketAddress(s));

		if(s instanceof ServerSocket){
			ServerConnector sc = new ServerConnector((ServerSocket)s, false);
			serverInConnections.add(sc);
		}
		else{
			ClientConnector cc = new ClientConnector(s);
			clientConnections.add(cc);
		}
		return null;*/

	}

	/*
	 * A new outgoing connection has been established, and a reference is returned
	 * to it
	 */
	public synchronized ServerConnector outgoingConnection(Socket s) throws IOException {
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		ServerConnector c = new ServerConnector(s, true);
		outGoingConnection = c;
		connections.add(c);
		broadConnections.add(c);

		String msg = Protocol.authenticate(Settings.getSecret(), false, 0);
		c.writeMsg(msg);

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
	private synchronized boolean register(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
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

				String msg = Protocol.lockRequest(username, secret);

				for (ServerConnector server : broadConnections) {
					server.writeMsg(msg);
				}
				return true;
			}

			// Otherwise this is a stand alone server, register success
			else {
				userInfo.put(username, secret);

				String msg = Protocol.registerSuccess("register successful for " + username);
				con.writeMsg(msg);
				return true;
			}
		}

		// Already registered in database, register fail
		String msg = Protocol.registerFailed(username + " is already register with the system");
		con.writeMsg(msg);
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
	private synchronized boolean lockProcess(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
		// Check if the message source is authenticated
		if (!broadConnections.contains(con)) {
			String msg = Protocol.invalidMessage("Unanthenticated server");
			con.writeMsg(msg);
			return false;
		}
		String username = receivedMSG.get("username").getAsString();
		String secret = receivedMSG.get("secret").getAsString();

		// Broadcast message to all other servers except the source
		if (!registerList1.containsKey(username)) {
			for (ServerConnector server : broadConnections) {
				if (server != con) {

					// If this is the server that asked for lock response from other server
					// for the coming user name, stop broadcasting

					server.writeMsg(receivedMSG.getAsString());
				}
			}
		} else {
			if (receivedMSG.get("command").getAsString().equals(Protocol.Type.LOCK_ALLOWED.name())) {
				int n = registerList1.get(username) + 1;
				registerList1.put(username, n);
				// If the number of allow reaches the number of connected server
				// send register success
				if (n >= serverInfo.size()) {
					userInfo.put(username, secret);

					String msg = Protocol.registerSuccess("register successful for  " + username);
					registerList2.get(username).writeMsg(msg);
					registerList1.remove(username);
					registerList2.remove(username);
					return true;
				}

			}
			// If received lock denied, send register failed immediately
			else if (receivedMSG.get("command").getAsString().equals(Protocol.Type.LOCK_DENIED.name())) {
				String msg = Protocol.registerFailed(username + " is already register with the system");
				registerList2.get(username).writeMsg(msg);
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
	private synchronized boolean lockRequest(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
		if (!broadConnections.contains(con)) {
			String msg = Protocol.invalidMessage("Unanthenticated server");
			con.writeMsg(msg);
			return false;
		}
		String username = receivedMSG.get("username").getAsString();
		String secret = receivedMSG.get("secret").getAsString();

		for (ServerConnector server : broadConnections) {
			if (server != con) {
				server.writeMsg(receivedMSG.getAsString());
			}
		}

		// If the user name does not exist in local database
		// store it now
		if (!userInfo.containsKey(username)) {

			userInfo.put(username, secret);

			String msg = Protocol.lockAllowed(username, secret);
			for (ServerConnector server : broadConnections) {
				server.writeMsg(msg);
			}

			return true;
		}
		// Otherwise return lock denied
		String msg = Protocol.lockDenied(username, secret);
		for (ServerConnector server : broadConnections) {
			server.writeMsg(msg);
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
	private synchronized boolean login(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {

		String msg;
		String secret = null;
		if (!receivedMSG.get("secret").isJsonNull())
			secret = receivedMSG.get("secret").getAsString();
		String username = receivedMSG.get("username").getAsString();
		int currentLoad = loadConnections.size();

		// If the user login as anonymous or has right name and secret
		// then send login success and check if need to redirect
		if ((username.equals(Protocol.ANONYMOUS_USERNAME) && secret == null)
				|| (userInfo.containsKey(username) && userInfo.get(username).equals(secret))) {

			msg = Protocol.loginSuccess("logged in as user " + username);
			if (!loadConnections.contains(con)) {
				loadConnections.add(con);
			}
			con.setUsername(username);
			con.setSecret(secret);
			con.writeMsg(msg);

			// Check for redirect
			if (!serverInfo.isEmpty()) {

				for (JsonObject info : serverInfo.values()) {

					String hostname = info.get("hostname").toString().replaceAll("\"", "");
					// System.out.print("hostname is " + hostname);
					int load = info.get("load").getAsInt();
					int port = info.get("port").getAsInt();
					if (load + 2 < currentLoad) {

						msg = Protocol.redirect(hostname, port);
						System.out.println("redirecting to " + msg);
						con.writeMsg(msg);
						con.closeCon();
						return false;
					}
				}
			}
		}
		// Else ask for other server about the login info
		else {
			loginList1.put(username, 0);
			loginList2.put(username, con);
			msg = Protocol.loginRequest(username, secret);
			for (ServerConnector connection : broadConnections) {
				connection.writeMsg(msg);
			}
		}
		return true;
	}

	private synchronized boolean loginProcess(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
		// Check if the message source is authenticated
		if (!broadConnections.contains(con)) {
			String msg = Protocol.invalidMessage("Unanthenticated server");
			con.writeMsg(msg);
			return false;
		}
		String username = receivedMSG.get("username").getAsString();
		String secret = receivedMSG.get("secret").getAsString();
		int currentLoad = loadConnections.size();

		// Broadcast message to all other servers except the source
		// Or stop if this is the source of login request
		if (!loginList1.containsKey(username)) {
			for (ServerConnector server : broadConnections) {
				if (server != con) {
					server.writeMsg(receivedMSG.toString());
				}
			}
		} else {
			if (receivedMSG.get("command").getAsString().equals(Protocol.Type.LOGIN_DENIED.name())) {
				int n = loginList1.get(username) + 1;
				loginList1.put(username, n);
				// If the number of denies reaches the number of connected server
				// send login fail
				if (n >= serverInfo.size()) {

					String msg = Protocol.loginFailed("Incorrect user info");
					loginList2.get(username).writeMsg(msg);
					loginList1.remove(username);
					loginList2.remove(username);
					return true;
				}

			}
			// If received any login allowed, send login success immediately and save info
			// in local database
			else if (receivedMSG.get("command").getAsString().equals(Protocol.Type.LOGIN_ALLOWED.name())) {
				String msg = Protocol.loginSuccess("logged in as user " + username);
				loginList2.get(username).writeMsg(msg);
				if (!userInfo.containsKey(username)) {
					userInfo.put(username, secret);
				}
				// Check for redirect
				if (!serverInfo.isEmpty()) {

					for (JsonObject info : serverInfo.values()) {
						String hostname = info.get("hostname").toString().replaceAll("\"", "");
						// System.out.print("hostname is " + hostname);
						int load = info.get("load").getAsInt();
						int port = info.get("port").getAsInt();
						if (load + 2 < currentLoad) {

							msg = Protocol.redirect(hostname, port);
							System.out.println("redirecting to " + msg);
							loginList2.get(username).writeMsg(msg);
							loginList2.get(username).closeCon();
							loginList1.remove(username);
							loginList2.remove(username);
							return false;
						}
					}
				}
				loginList1.remove(username);
				loginList2.remove(username);
			}
		}
		return true;
	}

	private synchronized boolean loginRequest(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
		if (!broadConnections.contains(con)) {
			String msg = Protocol.invalidMessage("Unanthenticated server");
			con.writeMsg(msg);
			return false;
		}
		String username = receivedMSG.get("username").getAsString();
		String secret = receivedMSG.get("secret").getAsString();

		for (ServerConnector server : broadConnections) {
			if (server != con) {
				server.writeMsg(receivedMSG.toString());
			}
		}

		// If the info matches the one stored in database, broadcast login allowed
		if (userInfo.containsKey(username)) {
			if (userInfo.get(username).equals(secret)) {
				String msg = Protocol.loginAllowed(username, secret);
				for (ServerConnector server : broadConnections) {
					server.writeMsg(msg);
				}
				return true;
			}
		}
		// Otherwise return login denied
		String msg = Protocol.loginDenied(username, secret);
		for (ServerConnector server : broadConnections) {
			server.writeMsg(msg);
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
	private synchronized boolean auth(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {

		String msg;
		// If already authenticated then reply with invalid message
		if (broadConnections.contains(con)) {
			msg = Protocol.invalidMessage("Already authenticated in this server");
			con.writeMsg(msg);
			return true;
		}
		// Check if provided secret matches the setting
		String secret = receivedMSG.get("secret").getAsString();
		if (!secret.equals(Settings.getSecret())) {
			msg = Protocol.authenticateFail("the supplied secret is incorrect: " + secret);
			con.writeMsg(msg);
			return false;
		}
		Timestamp reconnectTime = new Timestamp(0);
		ArrayList<String> messages = null;
		boolean isReconnect = receivedMSG.get("isReconnect").getAsBoolean();
		
		// If the coming message is for reconnect, send all messages that are 
		// broadcasted after it lost connection
		if (isReconnect) {
			Timestamp lostConnect = new Timestamp(receivedMSG.getAsLong());
			reconnectTime = new Timestamp(System.currentTimeMillis());
			messages = new ArrayList<String>();
			for (String user : messageLog.keySet()) {
				ArrayList<Message> allMsg = messageLog.get(user);
				for (Message message : allMsg) {
					if (message.getTime().after(lostConnect)) {
						messages.add(message.toString());
					}
				}
			}

		}
		if (Settings.getRemoteHostname() == null)
			msg = Protocol.authenticateSuccess(Settings.getLocalHostname(), Settings.getLocalPort(), "",
					Settings.getRemotePort(), isReconnect, reconnectTime.getTime(), messages);
		else
			msg = Protocol.authenticateSuccess(Settings.getLocalHostname(), Settings.getLocalPort(),
					Settings.getRemoteHostname(), Settings.getRemotePort(), isReconnect, reconnectTime.getTime(),
					messages);

		System.out.println(msg);
		con.writeMsg(msg);
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
	private synchronized boolean authSuccess(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {

		
		Settings.setBackupHostname(receivedMSG.get("remotehostname").getAsString());
		Settings.setBackupHostPort(receivedMSG.get("remotehostport").getAsInt());
		Settings.setRemoteHostname(receivedMSG.get("hostname").getAsString());
		Settings.setRemotePort(receivedMSG.get("hostport").getAsInt());
		System.out.print("Backup Server: " + Settings.getBackupHostname() + " " + Integer.toString(Settings.getBackupHostPort()) + "\n");


		// If the message is about authenticate reconnection, read the lost messages, store them and
		// re broadcast, then send messages that has not been broadcast to the other message
		if(receivedMSG.get("isReconnect").getAsBoolean()) {
			Timestamp reconnectTime = new Timestamp(receivedMSG.get("time").getAsLong());
			JsonArray jArray = new JsonArray();
			jArray = receivedMSG.getAsJsonArray("messages");
			// take the messages in json out
			for(int i = 0; i < jArray.size(); i ++) {
				String messageString = jArray.get(i).toString().replaceAll("\"", "").replaceAll("\\\\", "");
				JsonObject message = parser.parse(messageString).getAsJsonObject();
				Timestamp msgTime = new Timestamp(message.get("time").getAsLong());
				JsonObject actMsg = message.get("message").getAsJsonObject();
				Message msg = new Message(msgTime, actMsg);
				
				// Add message to local message database
				String username = actMsg.get("authenticated_user").getAsString();
				if(!messageLog.containsKey(username))
					messageLog.put(username, new ArrayList<Message>());
				
				ArrayList<Message> messages = messageLog.get(username);
				messages.add(msg);
				messageLog.put(username, messages);

				// Broadcast the messages
				for(ServerConnector connection : connections) {
					if(connection != con) {
						String broadcastMessage = Protocol.activityBroadcast(actMsg, msgTime.getTime());
						connection.writeMsg(broadcastMessage);
					}
				}
			}
			
			// Now look for messages in local that need re send
			ArrayList<String> messages = new ArrayList<String>();
			for (String user : messageLog.keySet()) {
				ArrayList<Message> allMsg = messageLog.get(user);
				for (Message message : allMsg) {
					if (message.getTime().after(disconnectTime) && message.getTime().before(reconnectTime)) {
						messages.add(message.toString());
					}
				}
			}
			
			String msg = Protocol.sendAllMessage(messages);
			con.writeMsg(msg);
		}
		//Update backup server for my node servers
		String msg;
		log.info("Number of server connected: " + broadConnections.size());
		for(ServerConnector connector : broadConnections){

			if(!con.equals(connector)){
				log.info("Rest server connection: " + connector);
				msg = Protocol.updateBackupHost(Settings.getRemoteHostname(),Settings.getRemotePort());
				connector.writeMsg(msg);
			}
		}
		return true;

	}
	
	
	
	public synchronized boolean sendAllMessage(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
		JsonArray jArray = new JsonArray();
		jArray = receivedMSG.getAsJsonArray("messages");
		// take the messages in json out
		for(int i = 0; i < jArray.size(); i ++) {
			JsonObject message = parser.parse(jArray.get(i).toString()).getAsJsonObject();
			Timestamp msgTime = new Timestamp(message.get("time").getAsLong());
			JsonObject actMsg = message.get("message").getAsJsonObject();
			Message msg = new Message(msgTime, actMsg);
			
			// Add message to local message database
			String username = actMsg.get("authenticated_user").getAsString();
			if(!messageLog.containsKey(username))
				messageLog.put(username, new ArrayList<Message>());
			
			ArrayList<Message> messages = messageLog.get(username);
			messages.add(msg);
			messageLog.put(username, messages);

			// Broadcast the messages
			for(ServerConnector connection : connections) {
				if(connection != con) {
					String broadcastMessage = Protocol.activityBroadcast(actMsg, msgTime.getTime());
					connection.writeMsg(broadcastMessage);
				}
			}
		}
		
		
		return true;
	}
	

	/**
	 * Process the coming server announce message, update local info
	 *
	 * @param con
	 * @param receivedMSG
	 * @return True if process successfully, false otherwise
	 */
	private synchronized boolean announce(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {

		if (!broadConnections.contains(con)) {
				String msg = Protocol.invalidMessage("Unanthenticated server");
				con.writeMsg(msg);
				return false;
			}
		
			// Broadcast announcement to other servers
			for (ServerConnector server : broadConnections) {
				if (con != server) {
					if (!receivedMSG.isJsonNull())
						server.writeMsg(receivedMSG.toString());
					else
						System.out.println("The received MSG is null");
				}
			}
			String hostname = receivedMSG.get("hostname").getAsString();
			serverInfo.put(hostname, receivedMSG);
			
		JsonArray jArray = new JsonArray();
		jArray = receivedMSG.getAsJsonArray("userInfo");
		// take the messages in json out
		for(int i = 0; i < jArray.size(); i ++) {
			String userString = jArray.get(i).toString().replaceAll("\"", "").replaceAll("\\\\", "");
			//System.out.println(userString);
			JsonObject user = parser.parse(userString).getAsJsonObject();
			String username = user.get("username").getAsString();
			String secret = user.get("secret").getAsString();
			if(!userInfo.containsKey(username))
				userInfo.put(username, secret);
		}

		return true;
	}

	/**
<<<<<<< HEAD
	 * Process the broadcast message from other servers
	 * 
=======
	 * Process the broadcast message from other servers Update their server info
	 *
>>>>>>> kageRedirectV1
	 * @param con
	 * @param receivedMSG
	 * @return True if the message source is authenticated, false otherwise
	 */
	private synchronized boolean broadcast(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
		if (!loadConnections.contains(con) && !broadConnections.contains(con)) {
			String msg = Protocol.invalidMessage("Unanthenticated connection");
			con.writeMsg(msg);
			return false;
		}

		Timestamp time = new Timestamp(receivedMSG.get("time").getAsLong());
		JsonObject actObj = receivedMSG.get("activity").getAsJsonObject();
		String username = actObj.get("authenticated_user").getAsString();
		
		// Add message to local log
		if (!messageLog.containsKey(username))
			messageLog.put(username, new ArrayList<Message>());
		ArrayList<Message> messages = messageLog.get(username);
		Message message = new Message(time,actObj);
		messages.add(message);
		messageLog.put(username, messages);
		String msg = Protocol.activityBroadcast(actObj, time.getTime());

		for (ServerConnector connection : connections) {
			if (connection != con)
				connection.writeMsg(msg);
		}
		return true;

	}

	/**
	 * Handles the activity message from client, add a time stamp, and store it to
	 * local message database
	 * 
	 * @param con
	 * @param receivedMSG
	 * @return
	 * @throws NullPointerException
	 */
	private synchronized boolean receiveMessage(ServerConnector con, JsonObject receivedMSG)
			throws NullPointerException {
		if (!loadConnections.contains(con)) {
			String msg = Protocol.invalidMessage("Unanthenticated connection");
			con.writeMsg(msg);
			return false;
		}

		Timestamp time = new Timestamp(System.currentTimeMillis());
		String username = receivedMSG.get("username").getAsString();

		String secret = null;
		if (!receivedMSG.get("secret").isJsonNull())
			secret = receivedMSG.get("secret").getAsString();
		if (secret == null || (con.getUsername().equals(username) && con.getSecret().equals(secret))) {

			if (!messageLog.containsKey(username))
				messageLog.put(username, new ArrayList<Message>());

			ArrayList<Message> messages = messageLog.get(username);

			// Process the activity object
			JsonObject actObj = receivedMSG.get("activity").getAsJsonObject();
			actObj.addProperty("authenticated_user", username);
			Message newMsg = new Message(time, actObj);
			messages.add(newMsg);
			messageLog.put(username, messages);
			String msg = Protocol.activityBroadcast(actObj, time.getTime());

			for (ServerConnector connection : connections) {
				connection.writeMsg(msg);
			}
			return true;
		} else {
			String msg = Protocol.authenticateFail("Unauthenticated connection");
			con.writeMsg(msg);
			return false;
		}
	}

	@SuppressWarnings("unchecked")
	private synchronized boolean updateBackup(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
		if (!broadConnections.contains(con)) {
			String msg = Protocol.invalidMessage("Unanthenticated connection");
			con.writeMsg(msg);
			return false;
		}

		if (receivedMSG.get("command").getAsString().equals(Protocol.Type.UPDATE_BACKUP.name())) {
			String backupHostName = receivedMSG.get("backuphostname").getAsString();
			int backupHostPort = receivedMSG.get("backupHostport").getAsInt();
			Settings.setBackupHostname(backupHostName);
			Settings.setBackupHostPort(backupHostPort);
			log.info("New backup host: " + backupHostName + " " + backupHostPort);
			return true;
		}
		return false;

	}

	@Override
	public void run() {
		// log.info("using activity interval of " + Settings.getActivityInterval() + "
		// milliseconds");
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
		for (ServerConnector connection : connections) {
			connection.closeCon();
		}
		for (ServerConnector connection : loadConnections) {
			connection.closeCon();
		}
		for (ServerConnector connection : broadConnections) {
			connection.closeCon();
		}
		listener.setTerm(true);
	}

	/**
	 * Broadcast the server states to other server
	 *
	 * @return
	 */
	public synchronized boolean doActivity() {
		
		String msg = Protocol.serverAnnounce(id, loadConnections.size(), Settings.getLocalHostname(),
				Settings.getLocalPort(),userInfo);

		for (ServerConnector cons : broadConnections) {
			cons.writeMsg(msg);
		}
		return false;
	}

	public final void setTerm(boolean t) {
		term = t;
	}

	public final ArrayList<ServerConnector> getConnections() {
		return connections;
	}

}
