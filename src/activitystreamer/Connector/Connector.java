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

import javax.swing.plaf.basic.BasicInternalFrameTitlePane;

public class Connector extends Thread{

    protected static final Logger log = LogManager.getLogger();

    protected DataInputStream in;
    protected DataOutputStream out;
    protected BufferedReader inReader;
    protected PrintWriter outWriter;
    protected Socket socket;

    protected boolean open;
    protected boolean term;

    public Connector(Socket socket) throws IOException {

        in = new DataInputStream(socket.getInputStream());
        out = new DataOutputStream(socket.getOutputStream());
        inReader = new BufferedReader(new InputStreamReader(in));
        outWriter = new PrintWriter(out, true);
        this.socket = socket;

        open = true;
        term = false;

        start();
    }

    public Socket getSocket(){
        return this.socket;
    }

    public boolean writeMsg(String msg) {

        if (open) {
            outWriter.println(msg);
            outWriter.flush();
            return true;
        }
        return false;
    }

    public void closeCon() {
        if (open) {
            log.info("closing connection " + Settings.socketAddress(socket));
            try {
                term = true;
                inReader.close();
                out.close();
            } catch (IOException e) {
                // already closed?
                log.error("received exception closing the connection " + Settings.socketAddress(socket) + ": " + e);
            }
        }
    }

}
