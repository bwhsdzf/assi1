package activitystreamer.server;

import com.google.gson.Gson;

public class InvalidMessage {

    private final static String INVALID_MESSAGE = "INVALID_MESSAGE";

    private String command;
    private String info;

    public InvalidMessage(){
        command = INVALID_MESSAGE;
    }

    public void setInfo(String info){
        this.info = info;
    }

    public String toJsonString() {
        return new Gson().toJson(this);
    }
}
