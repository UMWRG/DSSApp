package Resources;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONObject;

public class TimeSeries extends Resource{
	public List<TimeSeriesValue> ts_values = new ArrayList<TimeSeriesValue>();
	
	public TimeSeries(){

	}
	
	public TimeSeries(JSONObject json_timeseries){
		JSONArray ts_values = json_timeseries.getJSONArray("ts_values");
		for (int i=0;i<ts_values.length(); i++){
			JSONObject json_ts_val = ts_values.getJSONObject(i); 
			this.ts_values.add(new TimeSeriesValue(json_ts_val));
		}
	}
	
	public void add_value(DateTime ts_time, double ts_value){
		TimeSeriesValue ts_val = new TimeSeriesValue(ts_time, ts_value);
		ts_values.add(ts_val);
	}
	
	public JSONObject getAsJSON(){
		
		JSONObject json_ts = new JSONObject();
		for (TimeSeriesValue tsv : this.ts_values){
			json_ts.append("ts_values", tsv.getAsJSON());
		}
		return json_ts;
	
	}

	/*
	 * Get all the times in the timeseries as a list
	 */
	public DateTime[] get_times(){
		DateTime[] dt = new DateTime[this.ts_values.size()];
		for (int i=0; i<dt.length;i++){
			dt[i] = this.ts_values.get(i).ts_time;
		}
		return dt;
	}

	/*
	 * Get all the values in the tiemseries as a list
	 */
	public double[] get_values(){
		double[] vals = new double[this.ts_values.size()];
		for (int i=0; i<vals.length;i++){
			vals[i] = this.ts_values.get(i).ts_value;
		}
		return vals;
	}
}

class TimeSeriesValue extends Resource{
	public String format="yyyy-MM-dd HH:mm:ss";
	public DateTime ts_time;
	public double ts_value;
	
	public TimeSeriesValue(DateTime ts_time, double ts_value){
		this.ts_time = ts_time;
		this.ts_value = ts_value;
	}
	
	public TimeSeriesValue(JSONObject json_ts_val) {
		DateTimeFormatter formatter = DateTimeFormat.forPattern(this.format);
		SimpleDateFormat f = new SimpleDateFormat("ddMMMyyyy_HH:mm");
		this.ts_time = formatter.parseDateTime(json_ts_val.getJSONArray("ts_time").getString(0));
		//TODO: hack...
		JSONObject j = json_ts_val.getJSONArray("ts_value").getJSONObject(0);
		JSONObject k = j.getJSONArray("array").getJSONObject(0);
		this.ts_value = k.getJSONArray("item").getDouble(0);
	}

	public JSONObject getAsJSON(){
		JSONObject json_obj = new JSONObject();

		SimpleDateFormat formatter=new SimpleDateFormat(this.format);
		System.out.println(this.ts_time);
		json_obj.put("ts_time" , formatter.format(this.ts_time));
		json_obj.put("ts_value", this.ts_value);
		return json_obj;

		
	}
}
