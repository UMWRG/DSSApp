package Resources;

import java.lang.reflect.Field;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import JSONClientLib.HydraClientException;

/*
 * A dataset paired with Hydra. Has all the necessary attributes for communicating
 * datasets with Hydra.
 */
public class Dataset extends Resource{

	public int id;
	public String type;
	public String unit;
	public String dimension;
	public String name;
	public Object value;
	public Metadata[] metadata;
	
	public Dataset(){

	}
	
	public Dataset(String name, String type, String unit, String dimension){
		this.name = name;
		this.type = type;
		this.unit = unit;
		this.dimension = dimension;
	}
	
	public Dataset(JSONObject json_dataset) throws HydraClientException{
		this.name      = json_dataset.getString("name");
		this.type      = json_dataset.getString("type");
		
		JSONObject json_val = json_dataset.getJSONObject("value");

		if (this.type.equals("timeseries")){
			this.value = new TimeSeries(json_val);
		}else {
			throw new HydraClientException("Dataset " + json_dataset.getInt("id") + "is not a timeseries");
		}

		if (!json_dataset.get("unit").equals(null)){
			this.unit      = json_dataset.getString("unit");
		}else{
			this.unit      = null;
		}
		
		if (!json_dataset.get("dimension").equals(null)){
			this.dimension = json_dataset.getString("dimension");
		}else{
			this.dimension = null;
		}
		
		JSONArray metadata_array = json_dataset.getJSONArray("metadata");
		this.metadata = new Metadata[metadata_array.length()];
		
		for (int i=0; i<this.metadata.length;i++){
			this.metadata[i] = new Metadata(metadata_array.getJSONObject(i));
		}
	}
	
	public JSONObject getAsJSON(){
		
		JSONObject json_obj = new JSONObject();
		json_obj.put("name", this.name);
		json_obj.put("type", this.type);
		json_obj.put("unit", this.unit);
		json_obj.put("dimension", this.dimension);
		if (this.type == "timeseries"){
			TimeSeries ts = (TimeSeries) value;
			json_obj.put("value", ts.getAsJSON());
		}
		
		if (this.metadata != null){
			JSONArray metadata_json = new JSONArray();
			for(int i=0; i<this.metadata.length; i++){
				JSONObject j_meta = this.metadata[i].getAsJSON();
				metadata_json.put(j_meta);
			}
			json_obj.put("metadata", metadata_json);
		}
		return json_obj;
	}

	public String get_metadata(String key){
		for (Metadata m : this.metadata){
			if (m.name == key){
				return m.value;
			}
		}
		return null;
	}
	
	public int get_metadata(String key, int default_value){
		for (Metadata m : this.metadata){
			if (m.name == key){
				return Integer.parseInt(m.value);
			}
		}
		return default_value;
	}
	
	public String get_metadata(String key, String default_value){
		for (Metadata m : this.metadata){
			if (m.name == key){
				return m.value;
			}
		}
		return default_value;
	}
	
	public Double get_metadata(String key, Double default_value){
		for (Metadata m : this.metadata){
			if (m.name == key){
				return Double.parseDouble(m.value);
			}
		}
		return default_value;
	}
}
