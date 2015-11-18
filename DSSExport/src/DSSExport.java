import hec.heclib.dss.HecDss;
import hec.heclib.util.HecTime;
import hec.io.TimeSeriesContainer;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.Days;
import org.joda.time.Hours;
import org.joda.time.Minutes;
import org.joda.time.Months;
import org.joda.time.Years;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.json.JSONArray;
import org.json.JSONObject;

import JSONClientLib.HydraClientException;
import JSONClientLib.JSONConnector;
import Resources.Dataset;
import Resources.DatasetCollection;

public class DSSExport {

	int[] dataset_ids;
	String name;
	String session_id;
	String URL;
	JSONConnector connection;
	String output_folder;
	String output_file;
	public static int steps = 5;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// create Options object
		Options options = new Options();
		System.out.println("!!Progress 1/"+DSSExport.steps);
		String userHome             = System.getProperty("user.home");
		String defaultOutputFolder  = userHome + File.separator + "Desktop";
	    DateTimeFormatter file_name_format = DateTimeFormat.forPattern("ddMMMyyyyHHmm");
		String defaultOutputFile    = "output_"+file_name_format.print(DateTime.now())+".dss";
		
		// App-specific options
		options.addOption("d", true, "Dataset IDS");
		options.addOption("n", true, "Dataset Collection ID");
		options.addOption("o", true, "Output path");
		options.addOption("f", true, "Output file name");
		
		//Options required by all apps.
		options.addOption("c", true, "session ID");
		options.addOption("u", true, "server URL");
		options.addOption("h", true, "Help");
		
		
		
		String message = "Success";
		List<String> errors = new ArrayList<String>();
		List<String> warnings = new ArrayList<String>();
		List<String> files = new ArrayList<String>();

		CommandLineParser parser = new DefaultParser();
		DSSExport d = new DSSExport();
		try {
			String javaLibPath = System.getProperty("java.library.path");
			System.out.println("Java.library.path: " + javaLibPath);
			
			CommandLine cmd = parser.parse(options, args);
			
			d.session_id = cmd.getOptionValue('c', null);
			d.URL        = cmd.getOptionValue('u', "http://localhost:8080/json");
			
			//Write the help text if it's asked for.
			if (cmd.getOptionValue('c', null) != null){
				DSSExport.write_help();
			}
			
			d.connection = new JSONConnector(d.URL);
			if (d.session_id == null){
				d.connection.login("root", "");
			}else{
				d.connection.session_id = d.session_id;
			}

			String output_file_param = cmd.getOptionValue('f', defaultOutputFile);
			//Just get the file name from the path.
			File outputFile = new File(output_file_param);
			String fileName = outputFile.getName();
			d.output_file = fileName;
				
			//If a full filepath is specified for export, use that. If it's just
			//a file name, then use the given or default folder and the given name.
			if (output_file_param.contains("/") || output_file_param.contains("\\")){
				//We just want the path, not including the file name itself.
				d.output_folder = outputFile.getAbsolutePath().replace(fileName,"");
				System.out.println("Containing folder = " + d.output_folder);
			}else{	
				d.output_folder = cmd.getOptionValue('o', defaultOutputFolder);
			}	
						
			String full_output_path = d.output_folder + File.separator + d.output_file;
			
			System.out.println("!!Output Will output file to " + full_output_path);
			
			String dataset_id_string = cmd.getOptionValue('d', "");
			//Get all the dataset ids that have been passed in manually.
			//Assume they have been passed in comma-separated
			//Remove any spaces before doing anything. Numbers shouldn't have spaces!
			dataset_id_string = dataset_id_string.replace(" ", "");
			
			int[] dataset_ids;
			
			if (dataset_id_string != ""){
				String[] arg_dataset_ids = dataset_id_string.split(",");
				dataset_ids = new int[arg_dataset_ids.length];
				for (int i=0; i<dataset_ids.length; i++){
					dataset_ids[i] = Integer.parseInt(arg_dataset_ids[i]);
				}
			}else{
				dataset_ids = new int[0];
			}

			String collection_id = cmd.getOptionValue('n', null);
			
			if (dataset_ids == null || dataset_ids.length == 0){
				if (collection_id == null || collection_id == ""){
					message = "Error";
					throw new HydraClientException("No datasets specified!");
				}
			}
			System.out.println("!!Progress 2/"+DSSExport.steps);
			//ALl the dataset IDS in the collection
			int[] collection_ids;
			
			if (collection_id != null && collection_id != ""){
				DatasetCollection c = d.get_collection(Integer.parseInt(collection_id));
				collection_ids = c.dataset_ids;
			}else{
				collection_ids = new int[0];
			}
			
			//Eliminate duplicates
			for (int d_id : dataset_ids){
				if (ArrayUtils.contains(collection_ids, d_id)){
					ArrayUtils.remove(collection_ids, d_id);
				}
			}
			//Get all the datasets from the collection and those specified manually
			int[] all_dataset_ids = ArrayUtils.addAll(dataset_ids, collection_ids);

			System.out.println("!!Progress 3/"+DSSExport.steps);
			System.out.println("!!Getting datasets");

			Dataset[] datasets = d.get_datasets(all_dataset_ids);
			
			System.out.println("!!Progress 4/"+DSSExport.steps);
			System.out.println("!!Output Writing datasets to file " + full_output_path);
			
			d.write_dss_file(datasets, full_output_path);
			
			System.out.println("!!Progress 5/"+DSSExport.steps);
			message = "Success File "+ full_output_path +" Created";
			
		} catch (ParseException e) {
			e.printStackTrace();
			message = "Error";
			errors.add(e.getMessage());
		}catch (HydraClientException e) {
			e.printStackTrace();
			message = "Error";
			errors.add(e.getMessage());
		} catch (Exception e) {
			e.printStackTrace();
			message = "Error";
			errors.add(e.getMessage());
		}
		String[] e = new String[errors.size()];
		e = errors.toArray(e);
		String[] w = new String[warnings.size()];
		w = warnings.toArray(w);
		String[] f = new String[files.size()];
		f = files.toArray(f);
		d.connection.write_plugin_output("", new int[0], message, e, w, f);

	}
	
	
	public static void write_help(){
		System.out.println("Help will go here.");
	}
	
	/*
	 * Write the Hydra datasets to the dss files.
	 */
	public void write_dss_file(Dataset[] datasets, String file_name) throws Exception{

		//If the file isn't there, create it.
		File f = new File(file_name);
		if(!f.exists()) {
			FileOutputStream out = new FileOutputStream(file_name);
			out.close();
		}
		
		//Open up the dss file
		HecDss dss_file = HecDss.open(file_name);

		for (Dataset dataset : datasets){
			TimeSeriesContainer ts = new TimeSeriesContainer();
			
			//First off, get the value in
			Hashtable<DateTime, Double> timeseries = dataset.value;

			DateTime[] times_as_date = new DateTime[timeseries.size()];
			
			Set<DateTime> datetimes = timeseries.keySet();
			datetimes.toArray(times_as_date);
			
			int[] int_times = new int[times_as_date.length];
			
		    DateTimeFormatter fmt = DateTimeFormat.forPattern("dd MMM yyyy, HH:mm");

    		DateTime t0 = null;
    		DateTime t1 = null;
			for (int i=0;i<int_times.length;i++){
				DateTime ts_datetime = times_as_date[i];
				HecTime ts_hec_time = new HecTime(fmt.print(ts_datetime));
				if (i==0){t0 = ts_datetime;}
				if (i==1){t1 = ts_datetime;}
				int_times[i] = ts_hec_time.value();
			}
			HecTime test = new HecTime();
			test.set(int_times[0]);

			ts.times = int_times;
			//Convert the values into a dss-compatible format (array of doubles)
			double[] values = new double[timeseries.size()];
			ArrayList<Double> v = new ArrayList<Double>(timeseries.values());
			
			//Convert Double to double.
			for (int i=0; i<v.size(); i++){
				values[i] = v.get(i).doubleValue();
			}

			ts.values = values;
			
			ts.numberValues = values.length;

			/*
			 * Part               Description
			 * A                  Project, river, or basin name
			 * B                  Location
			 * C                  Data parameter
			 * D                  Starting date of block, in a nine - character military format
			 * E                  Time interval
			 * F                  Additional user - defined descriptive information
			 */
			if (StringUtils.countMatches(dataset.name, "/") != 7){
				String source = dataset.get_metadata("source", dataset.name);
				String interval = dataset.get_metadata("interval", get_interval(t1, t0));
				String location = dataset.get_metadata("location", "LOC");
				String attribute = dataset.get_metadata("attribute", "");
				
				DateTimeFormatter name_fmt = DateTimeFormat.forPattern("ddMMMyyyy");
				String start_time = name_fmt.print(t0).toUpperCase();
				System.out.println("Source: " + source);
				System.out.println("Location: " + location);
				
				String name = "/" +source +  "/" + location + "/" + attribute + "/"+start_time+"/" + interval + "/"+DateTime.now()+"/";
				System.out.println("Dataset name: " + name);
				ts.fullName = name;
			}else{
				ts.fullName = dataset.name;
			}

			//ts.units = dataset.unit;
			ts.interval = int_times[1] - int_times[0];
			//TODO THis is not efficient. Make metadata for datasets into a hashtable instead of a list 
			ts.watershed = dataset.get_metadata("watershed", "");
			ts.location = dataset.get_metadata("location", "");
			ts.parameter = dataset.get_metadata("parameter", "");
			ts.type = dataset.get_metadata("data_type", "");
			ts.supplementalInfo = dataset.get_metadata("supplemental_info", "");
			ts.subLocation = dataset.get_metadata("sub_location", "");
			ts.subParameter = dataset.get_metadata("sub_parameter", "");
			ts.subVersion = dataset.get_metadata("sub_version", "");
			ts.version = dataset.get_metadata("version", "");
			ts.coordinateID = dataset.get_metadata("coordinate_id", 0);
			ts.timeZoneID = dataset.get_metadata("timezone_id","");
			ts.xOrdinate = dataset.get_metadata("x_ordinate", 0.0);
			ts.yOrdinate = dataset.get_metadata("y_ordinate", 0.0);
			ts.zOrdinate = dataset.get_metadata("z_ordinate", 0.0);
			ts.coordinateSystem = dataset.get_metadata("coordinate_system", 0);
			ts.horizontalDatum = dataset.get_metadata("horizontal_datum", 0);
			ts.horizontalUnits = dataset.get_metadata("horizontal_units", 0);
			ts.verticalDatum = dataset.get_metadata("vertical_datum", 0);
			ts.verticalUnits = dataset.get_metadata("vertical_units", 0);
			
			dss_file.put(ts);
		}
	}
	
	/*
	 * Naive interval calculation. If the interval isn't set in metadata, try to guess it.
	 */
	private String get_interval(DateTime largerDatetime, DateTime smallerDateTime) throws HydraClientException{
		int year_diff  = Years.yearsBetween(smallerDateTime, largerDatetime).getYears();
		int month_diff  = Months.monthsBetween(smallerDateTime, largerDatetime).getMonths();
		int day_diff  = Days.daysBetween(smallerDateTime, largerDatetime).getDays();
		int hour_diff = Hours.hoursBetween(smallerDateTime, largerDatetime).getHours();
	    int min_diff  = Minutes.minutesBetween(smallerDateTime, largerDatetime).getMinutes();
	    
	    if (year_diff > 0){return year_diff+"YEAR";}
	    if (month_diff > 0){return month_diff+"MONTH";}
	    if (day_diff > 0){return day_diff+"DAY";}
	    if (hour_diff > 0){return hour_diff+"HOUR";}
	    if (min_diff > 0){return min_diff+"MIN";}

	    throw new HydraClientException("Could not compute interval between times " + smallerDateTime.toString() + "and" + largerDatetime.toString());
		
	}
	
	/*
	 * Create a dataset collection containing the newly ađded dataset IDS.
	 */
	public DatasetCollection get_collection(int collection_id) throws HydraClientException{
				
		JSONObject req_obj = new JSONObject();
		req_obj.put("collection_id", collection_id);
		
		System.out.println(req_obj);

		String str_collection = this.connection.call_function("get_dataset_collection", req_obj);
		
		JSONObject json_collection = new JSONObject(str_collection);
		
		return new DatasetCollection(json_collection);
		
	}
	
	public Dataset[] get_datasets(int[] dataset_ids) throws HydraClientException{
		//Make my JSON objects to put into the request
		JSONArray dataset_ids_json = new JSONArray(dataset_ids);
		JSONObject req_data = new JSONObject();
		req_data.put("dataset_ids", dataset_ids_json);
		//Call get datasts
		String datasets_res = this.connection.call_function("get_datasets", req_data);
		//Parse the result (should be an array of dataset objects)
		JSONArray ret_datasets_json = new JSONArray(datasets_res);
		Dataset[] ret_datasets = new Dataset[ret_datasets_json.length()];
		
		//Convert the JSON datasets into Client datasets
		for (int i=0; i<ret_datasets_json.length(); i++){
			ret_datasets[i] = new Dataset(ret_datasets_json.getJSONObject(i));
		}
		
		return ret_datasets;
	}
	
}
