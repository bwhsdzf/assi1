package activitystreamer.client;

import java.util.Scanner;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;


public class InputListener extends Thread{
	private Scanner sc;
	
	public InputListener() {
		sc = new Scanner(System.in);
		start();
		
	}
	
	
	public void run() {
		String inputStr;
		JSONParser parser = new JSONParser();
		while (!(inputStr = sc.nextLine()).equals("exit")) {
			try {
				ClientSkeleton.getInstance().sendActivityObject((JSONObject) parser.parse(inputStr));
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
	}
}