package activitystreamer.util;

import java.sql.Timestamp;
import com.google.gson.JsonObject;

public class Message {
	private Timestamp time;
	private JsonObject message;
	
	public Message(Timestamp time, JsonObject message) {
		this.time = time;
		this.message = message;
	}
	
	public Timestamp getTime() {
		return this.time;
	}
	
	public JsonObject getMessage() {
		return this.message;
	}
	
	public String toString() {
		JsonObject json = new JsonObject();
		json.addProperty("time", time.getTime());
		json.addProperty("message", message.toString());
		return json.toString();
	}

}
