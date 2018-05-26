package activitystreamer.Connector;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import activitystreamer.server.Control;
import activitystreamer.util.Settings;

public class ClientConnector extends Connector{

    private String userName;
    private String secret;

    public ClientConnector(Socket socket) throws IOException {

        super(socket);
        userName = null;
        secret = null;
        start();
    }

    public ClientConnector(Socket socket, String userName, String secret) throws IOException {

        super(socket);
        this.userName = userName;
        this.secret = secret;
        start();
    }

    public void setUsername(String username) {
        this.userName = username;
    }

    public void setSecret(String secret) {
        this.secret = secret;
    }

    public String getUsername() {
        return this.userName;
    }
    public String getSecret() {
        return this.secret;
    }

    public void run() {

    }

}
