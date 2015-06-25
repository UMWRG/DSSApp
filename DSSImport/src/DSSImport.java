import hec.heclib.dss.HecDss;
import hec.heclib.util.HecTime;
import hec.hecmath.HecMath;
import hec.io.DataContainer;
import hec.io.TimeSeriesContainer;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.joda.time.DateTime;
import org.json.JSONArray;
import org.json.JSONObject;

import JSONClientLib.HydraClientException;
import JSONClientLib.JSONConnector;
import Resources.Dataset;
import Resources.DatasetCollection;
import Resources.Metadata;
import Resources.TimeSeries;


public class DSSImport {

	int[] dataset_ids;
	String name;
	String session_id;
	String URL;
	JSONConnector connection;
	public static int steps = 5;
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// create Options object
		System.out.println("!!Progress 1/"+DSSImport.steps);

		Options options = new Options();

		// App-specific options
		options.addOption("f", true, "file path");
		options.addOption("n", true, "Collection Name");
		
		//Options required by all apps.
		options.addOption("c", true, "session ID");
		options.addOption("u", true, "server URL");
		
		
		String message = "Success";
		List<String> errors = new ArrayList<String>();
		List<String> warnings = new ArrayList<String>();
		List<String> files = new ArrayList<String>();

		CommandLineParser parser = new DefaultParser();
		DSSImport d = new DSSImport();
		try {
			String javaLibPath = System.getProperty("java.library.path");
			System.out.println("Java.library.path: " + javaLibPath);
			
			CommandLine cmd = parser.parse(options, args);
			
			String file_name = cmd.getOptionValue('f');
			
			d.session_id = cmd.getOptionValue('c', null);
			d.URL        = cmd.getOptionValue('u', "http://localhost:8080/json");
			
			d.connection = new JSONConnector(d.URL);
			if (d.session_id == null){
				d.connection.login("root", "");
			}else{
				d.connection.session_id = d.session_id;
			}
			
			if (file_name == null){
				message = "Error";
				throw new HydraClientException("No file specified.");
			}
			
			String collection_name = cmd.getOptionValue('n', 
					                "DSS import of " + file_name + DateTime.now().toString());
			
			System.out.println("!!Progress 2/"+DSSImport.steps);
			System.out.println("!!Output Reading DSS file.");
			
			List<Dataset> datasets = d.read_dss_file(file_name);

			System.out.println("!!Progress 3/"+DSSImport.steps);
			System.out.println("!!Output Saving datasets.");

			d.save_data(datasets);
			
			System.out.println("!!Progress 4/"+DSSImport.steps);
			System.out.println("!!Output Creating collection.");

			if (d.dataset_ids == null || d.dataset_ids.length == 0){
				warnings.add("No datasets found in file: " + file_name);
			}else{
				d.create_collection(collection_name, d.dataset_ids);
			}

			System.out.println("!!Progress 5/"+DSSImport.steps);

			message = "Success. Datasets saved in collection" + collection_name;
			
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
	
	public List<Dataset> read_dss_file(String file_name) throws Exception{
		List<Dataset> datasets = new ArrayList<Dataset>();
		
		//Open up the dss file
		HecDss dss_file = HecDss.open(file_name);

		//Get all the timeseries containers
		Vector<String> pathnames = dss_file.getCatalogedPathnames();
		Vector<TimeSeriesContainer> ts_containers = new Vector<TimeSeriesContainer>();
		for (int i=0; i<pathnames.size(); i++){
			HecMath hec_math = dss_file.read(pathnames.get(i).toString());
			DataContainer dc = hec_math.getData();
			//There are 2 types of data in a dss file. TimeSeriesContainer and PairedDataContainer
			//For the moment we're only interested in timeseries, so we isolate them.
			if (dc instanceof TimeSeriesContainer){
				TimeSeriesContainer ts = (TimeSeriesContainer) dc;
				ts_containers.add(ts);
			}
		}
		//Create a list of timeseries to send to the server.
		List<Dataset> all_timeseries = new ArrayList<Dataset>();
		for (int i=0; i<ts_containers.size();i++){
			
			Dataset dataset = new Dataset();
			
			TimeSeriesContainer ts = ts_containers.get(i);
			
			dataset.name      = ts.fullName;
			dataset.unit      = ts.units;
			dataset.type      = "timeseries";
			dataset.dimension = "";
			
			dataset.metadata = this.get_metadata(ts);
			
			int[] times = ts.times;
			double[] values = ts.values;
			TimeSeries hydra_ts = new TimeSeries();
			
			for (int j=0; j< times.length; j++){
				int time = times[j];
				double value = values[j];
				HecTime hectime = new HecTime();
				hectime.set(time);
				hectime.date();
				System.out.println(hectime.toString());
				DateTime d = new DateTime(hect);
				hydra_ts.add_value(d, value);
			}
			
			dataset.value = hydra_ts;
			
			all_timeseries.add(dataset);
		}
		dss_file.done();
		return all_timeseries;

	}
	
	
	private Metadata[] get_metadata(TimeSeriesContainer hec_timeseries) throws HydraClientException{
		Metadata[] m = {
		new Metadata("source", "DSS Import"),
		new Metadata("watershed", hec_timeseries.watershed),
		new Metadata("location", hec_timeseries.location),
		new Metadata("parameter", hec_timeseries.parameter),
		new Metadata("file_name", hec_timeseries.fileName),
		new Metadata("data_type", hec_timeseries.type),
		new Metadata("supplemental_info", hec_timeseries.supplementalInfo),
		new Metadata("sub_location", hec_timeseries.subLocation),
		new Metadata("sub_parameter", hec_timeseries.subParameter),
		new Metadata("sub_version", hec_timeseries.subVersion),
		new Metadata("version", hec_timeseries.version),
		new Metadata("coordinate_id", ""+hec_timeseries.coordinateID),
		new Metadata("timezone_id", hec_timeseries.timeZoneID),
		new Metadata("x_ordinate", ""+hec_timeseries.xOrdinate),
		new Metadata("y_ordinate", ""+hec_timeseries.yOrdinate),
		new Metadata("z_ordinate", ""+hec_timeseries.zOrdinate),
		new Metadata("coordinate_system", ""+hec_timeseries.coordinateSystem),
		new Metadata("horizontal_datum", ""+hec_timeseries.horizontalDatum),
		new Metadata("horizontal_units", ""+hec_timeseries.horizontalUnits),
		new Metadata("vertical_datum", ""+hec_timeseries.verticalDatum),
		new Metadata("vertical_units", ""+hec_timeseries.verticalUnits)
		};
		
		return m;
	}
	
	/*
	 * Create a dataset collection containing the newly ađded dataset IDS.
	 */
	public void create_collection(String name, int[] dataset_ids) throws HydraClientException{
		
		DatasetCollection coll = new DatasetCollection();
		coll.dataset_ids = dataset_ids;
		coll.name        = name;
		
		System.out.println("Name: " + name);
		
		JSONObject req_obj = new JSONObject();
		req_obj.put("collection", coll.getAsJSON());
		
		System.out.println(req_obj);

		String ids = this.connection.call_function("add_dataset_collection", req_obj);
		
		System.out.println(ids);

		
	}
	
	/*
	 * Save all the datasets found in the dss file to Hydra.
	 */
	public void save_data(List<Dataset> datasets) throws HydraClientException{
		//Don't try to save if there are no datasets to save
		if (datasets.size() == 0){
			System.out.println("No datasets to save!");
			return;
		}
		
		JSONObject all_datasets = new JSONObject();
		System.out.println(datasets.size());
		for (Dataset d : datasets){
			JSONObject json_dataset = d.getAsJSON();
			all_datasets.append("bulk_data", json_dataset);
		}
		String ids = this.connection.call_function("bulk_insert_data", all_datasets);
		JSONArray dataset_ids = new JSONArray(ids);
		
		int[] dataset_id_array = new int[dataset_ids.length()];
		for (int i=0; i<dataset_ids.length(); i++){
			dataset_id_array[i] = dataset_ids.getInt(i);
		}
		this.dataset_ids = dataset_id_array;
		
	}
}
