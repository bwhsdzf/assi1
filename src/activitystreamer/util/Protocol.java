package activitystreamer.util;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

import java.awt.*;
import java.util.ArrayList;


public class Protocol {

    public enum Type{
        INVALID_MESSAGE,
        AUTHENTICATE,
        AUTHENTICATION_SUCCESS,
        AUTHTENTICATION_FAIL,
        LOGIN,
        LOGIN_SUCCESS,
        LOGIN_FAILED,
        LOGIN_REQUEST,
        LOGIN_ALLOWED,
        LOGIN_DENIED,
        LOGOUT,
        REDIRECT,
        ACTIVITY_MESSAGE,
        ACTIVITY_BROADCAST,
        SERVER_ANNOUNCE,
        REGISTER,
        REGISTER_SUCCESS,
        REGISTER_FAILED,
        LOCK_REQUEST,
        LOCK_DENIED,
        LOCK_ALLOWED,
        SEND_ALL_MESSAGE
    }
    public final static String ANONYMOUS_USERNAME = "anonymous";

    //Encode JSON String.
    public static String invalidMessage(String info){
        JSONObject json = new JSONObject();
        json.put("command", Type.INVALID_MESSAGE.name());
        json.put("info", info);
        return json.toJSONString();
    }
    public static String authenticate(String secret, boolean isReconnect, long time){
        JSONObject json = new JSONObject();
        json.put("command", Type.AUTHENTICATE.name());
        json.put("secret", secret);
        json.put("reconnect", isReconnect);
        json.put("time", time);
        return json.toJSONString();
    }
    public static String authenticateSuccess(String myHostName, int myPort, String parentHostName, int hostPort,
    		boolean isReconnect, long time, ArrayList<String> messages){
        System.out.println("Root prepare authenticateSuccess");
        JSONObject json = new JSONObject();
        json.put("command", Type.AUTHENTICATION_SUCCESS.name());
        json.put("myhostname", myHostName);
        json.put("myport", myPort);
        json.put("parenthostname", parentHostName);
        json.put("parentport", hostPort);
        if(isReconnect) {
        	json.put("isReconnect", true);
        	json.put("time", time);
        	json.put("messages", messages);
        }
        return json.toJSONString();
    }
    public static String sendAllMessage(ArrayList<String> messages) {
    	JSONObject json = new JSONObject();
    	json.put("command", Type.SEND_ALL_MESSAGE.name());
    	json.put("messages", messages);
    	return json.toJSONString();
    }
    
    public static String authenticateFail(String info){
        JSONObject json = new JSONObject();
        json.put("command", Type.AUTHTENTICATION_FAIL.name());
        json.put("info", info);
        return json.toJSONString();
    }
    public static String login(String username, String secret){
        JSONObject json = new JSONObject();
        json.put("command", Type.LOGIN.name());
        json.put("username", username);
        json.put("secret", secret);
        return json.toJSONString();
    }
    public static String loginRequest(String username, String secret) {
    	JSONObject json = new JSONObject();
    	json.put("command", Type.LOGIN_REQUEST.name());
    	json.put("username", username);
    	json.put("secret", secret);
    	return json.toJSONString();
    }
    public static String loginAllowed(String username, String secret) {
    	JSONObject json = new JSONObject();
    	json.put("command", Type.LOGIN_ALLOWED.name());
    	json.put("username", username);
    	json.put("secret", secret);
    	return json.toJSONString();
    }
    public static String loginDenied(String username, String secret) {
    	JSONObject json = new JSONObject();
    	json.put("command", Type.LOGIN_DENIED.name());
    	json.put("username", username);
    	json.put("secret", secret);
    	return json.toJSONString();
    }
    public static String loginSuccess(String info){
        JSONObject json = new JSONObject();
        json.put("command", Type.LOGIN_SUCCESS.name());
        json.put("info", info);
        return json.toJSONString();
    }
    public static String loginFailed(String info){
        JSONObject json = new JSONObject();
        json.put("command", Type.LOGIN_FAILED.name());
        json.put("info", info);
        return json.toJSONString();
    }
    public static String logout(){
        JSONObject json = new JSONObject();
        json.put("command", Type.LOGOUT.name());
        return json.toJSONString();
    }
    public static String redirect(String hostname, int port){
        JSONObject json = new JSONObject();
        json.put("command", Type.REDIRECT.name());
        json.put("hostname", hostname);
        json.put("port", port);
        return json.toJSONString();
    }
    public static String activityMessage(String username, String secret, JsonObject activity){
        JSONObject json = new JSONObject();
        json.put("command", Type.ACTIVITY_MESSAGE.name());
        json.put("username", username);
        json.put("secret", secret);
        json.put("activity", activity);
        return json.toJSONString();
    }
    public static String activityBroadcast(JsonObject activity, long time){
        JSONObject json = new JSONObject();
        json.put("command", Type.ACTIVITY_BROADCAST.name());
        json.put("activity", activity);
        json.put("time", time);
        return json.toJSONString();
    }
    public static String serverAnnounce(String id, int load, String hostname, int port){
        JSONObject json = new JSONObject();
        json.put("command", Type.SERVER_ANNOUNCE.name());
        json.put("id", id);
        json.put("load", load);
        json.put("hostname", hostname);
        json.put("port", port);
        return json.toJSONString();
    }
    public static String register(String username, int port){
        JSONObject json = new JSONObject();
        json.put("command", Type.REGISTER.name());
        json.put("username", username);
        json.put("port", port);
        return json.toJSONString();
    }
    public static String registerSuccess(String info){
        JSONObject json = new JSONObject();
        json.put("command", Type.REGISTER_SUCCESS.name());
        json.put("info", info);
        return json.toJSONString();
    }
    public static String registerFailed(String info){
        JSONObject json = new JSONObject();
        json.put("command", Type.REGISTER_FAILED.name());
        json.put("info", info);
        return json.toJSONString();
    }
    public static String lockRequest(String username, String secret){
        JSONObject json = new JSONObject();
        json.put("command", Type.LOCK_REQUEST.name());
        json.put("username", username);
        json.put("secret", secret);
        return json.toJSONString();
    }
    public static String lockDenied(String username, String secret){
        JSONObject json = new JSONObject();
        json.put("command", Type.LOCK_DENIED.name());
        json.put("username", username);
        json.put("secret", secret);
        return json.toJSONString();
    }
    public static String lockAllowed(String username, String secret){
        JSONObject json = new JSONObject();
        json.put("command", Type.LOCK_ALLOWED.name());
        json.put("username", username);
        json.put("secret", secret);
        return json.toJSONString();
    }

    //Decode JSON String.




}
