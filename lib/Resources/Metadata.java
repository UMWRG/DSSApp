package Resources;

import java.text.SimpleDateFormat;

import org.json.JSONObject;

/*
 * Dataset metadata. Key-value pairs.
 */
public class Metadata extends Resource{
	public String name;
	public String value;
	
	public Metadata(){
		
	}
	
	public Metadata(JSONObject json_metadata){
		this.name = json_metadata.getString("name");
		this.value = json_metadata.getString("value");
	}
	
	public Metadata(String name, String value){
		this.name = name;
		if (value == null){
			this.value = "";
		}else{
			this.value = value;
		}	
	}
	
	public JSONObject getAsJSON(){
		JSONObject json_obj = new JSONObject();

		json_obj.put("name" , this.name);
		json_obj.put("value", this.value);
		
		return json_obj;
	}

}
