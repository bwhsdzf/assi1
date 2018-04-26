package activitystreamer.server;

import org.json.simple.JSONObject;

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

    @SuppressWarnings("unchecked")
	public String toJsonString() {
        JSONObject json = new JSONObject();
        json.put("command", command);
        json.put("info", info);
        return json.toJSONString();
    }
}
