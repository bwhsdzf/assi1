package activitystreamer.Connector;

import java.io.IOException;
import java.net.Socket;
import java.sql.Timestamp;

import activitystreamer.server.Control;
import activitystreamer.util.Settings;

public class ServerConnector extends Connector {

    //Record the login state of the client connection
    //
    // Can remove after modify
    private String username;
    private String secret;

    public ServerConnector(Socket socket, boolean isOutGoingConnection) throws IOException {
        super(socket);
        Settings.setIsOutGoingConnection(isOutGoingConnection);
        //NEED to DELETING
        username = null;
        secret = null;
    }

    //Can remove
    public void setUsername(String username) {
        this.username = username;
    }
    //Can remove
    public void setSecret(String secret) {
        this.secret = secret;
    }
    //Can remove
    public String getUsername() {
        return this.username;
    }
    //Can remove
    public String getSecret() {
        return this.secret;
    }


    public void run() {
        try {
            String data;
            while (!term && (data = inReader.readLine()) != null) {
                System.out.println(data);
                term = Control.getInstance().process(this, data);
            }
            log.debug("connection closed to " + Settings.socketAddress(socket));
            Control.getInstance().connectionClosed(this);
            in.close();
        } catch (IOException e) {

            if(Settings.getIsOutGoingConnection()) {
            	Timestamp time = new Timestamp(System.currentTimeMillis());
            	Control.getInstance().setDisconectTime(time);
                log.error("connection " + Settings.socketAddress(socket) + " disconnect. Reconnection");
                ServerConnector con = Control.getInstance().reconnect(this);
                
                while(con == null) {
                	try {
						Thread.sleep(10000);
						con = Control.getInstance().reconnect(con);
					} catch (InterruptedException e1) {
						continue;
					}
                }
            }

            else{
                log.error("connection " + Settings.socketAddress(socket) + " closed with exception: " + e);
                Control.getInstance().connectionClosed(this);
            }
        }
        //open = false;
    }

}
