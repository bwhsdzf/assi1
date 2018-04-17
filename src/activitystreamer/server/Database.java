package activitystreamer.server;

import java.util.ArrayList;

public class Database{
	private ArrayList<ConnectionInfo> connectionList;
	
	public class ConnectionInfo {
		private String username;
		private String secret;
		
		public ConnectionInfo(String username, String secret) {
			this.username = username;
			this.secret = secret;
		}
		
		public String getUsername() {
			return this.username;
		}
		
		public String getSecret() {
			return this.secret;
		}
	}
	
	public Database() {
		this.connectionList = new ArrayList<ConnectionInfo>();
		
	}
	
	public boolean checkAuthentication(String name, String secret) {
		return false;
	}
	
	public void addNew(String name, String secret) {
		this.connectionList.add(new ConnectionInfo(name,secret));
	}
}

