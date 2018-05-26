package activitystreamer.Connector;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.time.Instant;

import activitystreamer.server.Control;
import activitystreamer.util.Settings;

public class ServerConnector extends Connector {

    private boolean isOutGoingConnection;
    private boolean isRootBackupConnection;
    //Record the login state of the client connection
    //
    // Can remove after modify
    private String username;
    private String secret;

    private String inComingServerName = null;
    private int inComingServerPort = 0;
    private ConnectorType connectorType = ConnectorType.CLIENT_IN;

    public void setInComingServerName(String inComingServerName){
        this.inComingServerName=inComingServerName;
    }
    public String getInComingServerName(){
        return this.inComingServerName;
    }
    public void setInComingServerPort(int inComingServerPort){
        this.inComingServerPort = inComingServerPort;
    }
    public int getInComingServerPort(){
        return this.inComingServerPort;
    }
    public enum ConnectorType{
        SERVER_OUT,
        SERVER_IN_BACKUP,
        SERVER_IN_NORMAL,
        CLIENT_IN
    }
    public void setConnectorType(ConnectorType connectorType){
        this.connectorType = connectorType;
    }
    public ConnectorType getConnectorType(){
        return this.connectorType;
    }
    public ServerConnector(Socket socket, boolean isOutGoingConnection) throws IOException {
        super(socket);
        this.isOutGoingConnection = isOutGoingConnection;
        //NEED to DELETING
        username = null;
        secret = null;
    }
 /*   public ServerConnector(ServerSocket serverSocket, boolean isOutGoingConnection) throws IOException {
        super(serverSocket);
        this.isOutGoingConnection = isOutGoingConnection;
        //NEED to DELETING
        username = null;
        secret = null;
    }*/

    public void setIsOutGoingConnection(boolean isOutGoingConnection){
        this.isOutGoingConnection = isOutGoingConnection;
    }
    public boolean getIsOutGoingConnection(){
        return this.isOutGoingConnection;
    }
    public void setIsRootBackupConnection(boolean isRootBackupConnection){
        this.isRootBackupConnection = isRootBackupConnection;
    }
    public boolean getIsRootBackupConnection(){
        return this.isRootBackupConnection;
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

            /*if(isOutGoingConnection) {
                log.error("connection " + Settings.socketAddress(socket) + " disconnect. Reconnection");
                Control.getInstance().reconnect(this);
            }
            else{
                log.error("connection " + Settings.socketAddress(socket) + " closed with exception: " + e);
                Control.getInstance().connectionClosed(this);
            }*/
            log.error("connection " + Settings.socketAddress(socket) + " disconnect. Reconnection");
            Control.getInstance().reconnect(this);
        }
        //open = false;
    }

}
