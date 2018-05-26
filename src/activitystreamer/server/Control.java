package activitystreamer.server;

import java.io.IOException;
import java.net.Socket;
import java.util.*;

import activitystreamer.Connector.Connector;
import activitystreamer.Server;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import activitystreamer.Connector.ServerConnector;
import activitystreamer.Connector.ClientConnector;
import activitystreamer.util.Settings;
import activitystreamer.util.Protocol;

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

	private static boolean term = false;
	private static Listener listener;

	// Database that record the user login information
	private static HashMap<String, String> userInfo = new HashMap<>();

	// The server id
	private String id = Settings.nextSecret();

	protected static Control control = null;

	private JsonParser parser = new JsonParser();

	//Finalized
	public static Control getInstance() {
		if (control == null) {
			control = new Control();
		}
		return control;
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

	//Finalized
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

			if(type.equals(Protocol.Type.REGISTER.name())) {
				System.out.println(type);
				return !register(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.LOGIN.name())) {
				System.out.println(type);
				return !login(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.AUTHENTICATE.name())) {
				System.out.println(type);
				return !auth(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.AUTHENTICATION_SUCCESS.name())) {
				System.out.println(type);
				return !authSuccess(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.INVALID_MESSAGE.name())) {
				System.out.println(type);
				con.closeCon();
				return false;
			}
			else if(type.equals(Protocol.Type.SERVER_ANNOUNCE.name())) {
				System.out.println(type);
				return !announce(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.LOGOUT.name())) {
				System.out.println(type);
				con.closeCon();
				return false;
			}
			else if(type.equals(Protocol.Type.LOCK_REQUEST.name())) {
				System.out.println(type);
				return !lockRequest(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.LOCK_DENIED.name())) {
				System.out.println(type);
				return !lockProcess(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.LOCK_ALLOWED.name())) {
				System.out.println(type);
				return !lockProcess(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.ACTIVITY_BROADCAST.name())) {
				System.out.println(type);
				return !broadcast(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.ACTIVITY_MESSAGE.name())) {
				System.out.println(type);
				return !broadcast(con, receivedMSG);
			}
			else if(type.equals(Protocol.Type.AUTHTENTICATION_FAIL.name())) {
				System.out.println(type);
				con.closeCon();
				return false;
			}
			else if(type.equals(Protocol.Type.UPDATE_BACKUP.name())){
				System.out.println(type);
				return !updateBackup(con,receivedMSG);
			}

			else
				return false;
		} catch (NullPointerException e) {
			e.printStackTrace();
			msg = Protocol.invalidMessage("Not enough info in message," + " possibly no authenticated user in ACTIVITY_BROADCAST ?");
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
			String msg = Protocol.authenticate(Settings.getSecret());
			c.writeMsg(msg);

			log.debug("This message should appear after get new backup");

		}catch (IOException e) {
			log.error("failed to make RE-connection to " + Settings.getRemoteHostname() + ":"
					+ Settings.getRemotePort() + " :" + e);
			System.exit(-1);
		}
		return con;

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
	@SuppressWarnings("unchecked")
	public synchronized ServerConnector outgoingConnection(Socket s) throws IOException {
		log.debug("outgoing connection: " + Settings.socketAddress(s));
		ServerConnector c = new ServerConnector(s, true);
		outGoingConnection = c;
		connections.add(c);
		broadConnections.add(c);

		String msg = Protocol.authenticate(Settings.getSecret());
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
	@SuppressWarnings("unchecked")
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
	@SuppressWarnings("unchecked")
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
		for (ServerConnector server : broadConnections) {
			if (server != con) {

				// If this is the server that asked for lock response from other server
				// for the coming user name, stop broadcasting
				if (!registerList1.containsKey(username))
					server.writeMsg(receivedMSG.getAsString());
			}
		}

		if (receivedMSG.get("command").getAsString().equals(Protocol.Type.LOCK_ALLOWED.name())) {
			if (registerList1.containsKey(username)) {
				int n = registerList1.get(username) + 1;
				registerList1.put(username, n);
				// If the number of allow reaches the number of connected server
				// send register success
				if (n == serverInfo.size()) {
					userInfo.put(username, secret);

					String msg = Protocol.registerSuccess("register successful for  " + username);
					registerList2.get(username).writeMsg(msg);
					registerList1.remove(username);
					registerList2.remove(username);
					return true;
				}
			}

		}
		// If received lock denied, send register failed immediately
		else if (receivedMSG.get("command").getAsString().equals(Protocol.Type.LOCK_DENIED.name())) {
			if (registerList1.containsKey(username)) {
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
	@SuppressWarnings("unchecked")
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
	@SuppressWarnings("unchecked")
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
					//System.out.print("hostname is " + hostname);
					int load = info.get("load").getAsInt();
					int port = info.get("port").getAsInt();
					if (load + 2 < currentLoad) {

						msg = Protocol.redirect(hostname,port);
						System.out.println("redirecting to " + msg);  //////////////////////////////////////////////////////////////////////////////////////////////////////////
						con.writeMsg(msg);
						con.closeCon();
						return false;
					}
				}
			}
		}
		// Else login fail
		else {

			msg = Protocol.loginFailed("attempt to login with wrong secret");
			con.writeMsg(msg);
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
		if(Settings.getRemoteHostname()==null)
			msg = Protocol.authenticateSuccess(Settings.getLocalHostname(), Settings.getLocalPort(), "", Settings.getRemotePort());
		else
			msg = Protocol.authenticateSuccess(Settings.getLocalHostname(), Settings.getLocalPort(), Settings.getRemoteHostname(), Settings.getRemotePort());

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
	@SuppressWarnings("unchecked")
	private synchronized boolean authSuccess(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {

		Settings.setBackupHostname(receivedMSG.get("remotehostname").getAsString());
		Settings.setBackupHostPort(receivedMSG.get("remotehostport").getAsInt());
		Settings.setRemoteHostname(receivedMSG.get("hostname").getAsString());
		Settings.setRemotePort(receivedMSG.get("hostport").getAsInt());
		System.out.print("Backup Server: " + Settings.getBackupHostname() + " " + Integer.toString(Settings.getBackupHostPort()) + "\n");


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


		//
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
		try {
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
						System.out.println("The received MSG is null, baby");
				}
			}
			String hostname = receivedMSG.get("hostname").getAsString();
			serverInfo.put(hostname, receivedMSG);
		}
		catch (Exception e){
			System.out.println(receivedMSG.toString() + " What the fuck is going on/n");
		}
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
	private synchronized boolean broadcast(ServerConnector con, JsonObject receivedMSG) throws NullPointerException {
		if (!loadConnections.contains(con) && !broadConnections.contains(con)) {
			String msg = Protocol.invalidMessage("Unanthenticated connection");
			con.writeMsg(msg);
			return false;
		}

		// System.out.println("Broadcasting");
		if (receivedMSG.get("command").getAsString().equals(Protocol.Type.ACTIVITY_MESSAGE.name())) {
			String username = receivedMSG.get("username").getAsString();

			String secret = null;
			if (!receivedMSG.get("secret").isJsonNull())
				secret = receivedMSG.get("secret").getAsString();
			if (secret == null || (con.getUsername().equals(username) && con.getSecret().equals(secret))) {

				// Process the activity object
				JsonObject actObj = receivedMSG.get("activity").getAsJsonObject();
				actObj.addProperty("authenticated_user", username);
				String msg = Protocol.activityBroadcast(actObj);  ///////////////////////////////////////////////////////////////

				for (ServerConnector connection : connections) {
					connection.writeMsg(msg);
				}
				return true;
			} else {
				String msg = Protocol.authenticateFail("Unauthenticated connection");
				con.writeMsg(msg);
				return false;
			}
		} else if (receivedMSG.get("command").getAsString().equals(Protocol.Type.ACTIVITY_BROADCAST)) {


			JsonObject actObj = receivedMSG.get("activity").getAsJsonObject();
			actObj.addProperty("authenticated_user", actObj.get("authenticated_user").getAsString());

			String msg = Protocol.activityBroadcast(actObj);
			for (ServerConnector connection : connections) {
				if (connection != con)
					connection.writeMsg(msg);
			}
			return true;
		}
		return false;

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
	@SuppressWarnings("unchecked")
	public synchronized boolean doActivity() {

		String msg = Protocol.serverAnnounce(id, loadConnections.size(), Settings.getLocalHostname(), Settings.getLocalPort());

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
