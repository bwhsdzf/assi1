package activitystreamer.util;

import java.math.BigInteger;
import java.net.Socket;
import java.security.SecureRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Settings {
	private static final Logger log = LogManager.getLogger();
	private static SecureRandom random = new SecureRandom();
	private static int localPort = 3780;
	private static String localHostname = "localhost";
	private static String remoteHostname = null;
	private static int remotePort = 3780;
	private static String backupHostName = null;
	private static int backupHostPort = 0;
	private static String backupRootHostName = null;
	private static int backupRootHostPort = 0;

	/*private static boolean isOutGoingConnection = false;*/
	private static int activityInterval = 5000; // milliseconds
	private static String secret = null;
	private static String username = "anonymous";

	public static void setBackupRootHostName(String backupRootHostName) {
		Settings.backupRootHostName = backupRootHostName;
	}
	public static String getBackupRootHostName(){
		return backupRootHostName;
	}
	public static void setBackupRootHostPort(int backupRootHostPort){
		Settings.backupRootHostPort = backupRootHostPort;
	}
	public static int getBackupRootHostPort(){
		return backupRootHostPort;
	}

	public static int getLocalPort() {
		return localPort;
	}

	public static void setLocalPort(int localPort) {
		if(localPort<0 || localPort>65535){
			log.error("supplied port "+localPort+" is out of range, using "+getLocalPort());
		} else {
			Settings.localPort = localPort;
		}
	}
	
	public static int getRemotePort() {
		return remotePort;
	}

	public static void setRemotePort(int remotePort) {
		if(remotePort<0 || remotePort>65535){
			log.error("supplied port "+remotePort+" is out of range, using "+getRemotePort());
		} else {
			Settings.remotePort = remotePort;
		}
	}
	
	public static String getRemoteHostname() {
		return remoteHostname;
	}

	public static void setRemoteHostname(String remoteHostname) {
		Settings.remoteHostname = remoteHostname;
	}

	public static void setBackupHostname (String backupHostname) {
		Settings.backupHostName = backupHostname;
	}

	public static String getBackupHostname () {
		return backupHostName;
	}

	public static void setBackupHostPort (int backupHostPort) {
		Settings.backupHostPort = backupHostPort;
	}

	public static int getBackupHostPort () {
		return Settings.backupHostPort;
	}
	
	public static int getActivityInterval() {
		return activityInterval;
	}

	public static void setActivityInterval(int activityInterval) {
		Settings.activityInterval = activityInterval;
	}
	
	public static String getSecret() {
		return secret;
	}

	public static void setSecret(String s) {
		secret = s;
	}
	
	public static String getUsername() {
		return username;
	}

	public static void setUsername(String username) {
		Settings.username = username;
	}
	
	public static String getLocalHostname() {
		return localHostname;
	}

	public static void setLocalHostname(String localHostname) {
		Settings.localHostname = localHostname;
	}

	
	/*
	 * some general helper functions
	 */
	
	public static String socketAddress(Socket socket){
		return socket.getInetAddress()+":"+socket.getPort();
	}

	public static String nextSecret() {
	    return new BigInteger(130, random).toString(32);
	 }



	
}
