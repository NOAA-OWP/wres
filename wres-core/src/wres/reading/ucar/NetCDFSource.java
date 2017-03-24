/**
 * 
 */
package wres.reading.ucar;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import ucar.nc2.Attribute;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.reading.BasicSource;
import wres.util.Database;
import wres.util.Utilities;

/**
 * @author ctubbs
 *
 */
public class NetCDFSource extends BasicSource {

	/**
	 * 
	 */
	public NetCDFSource(String filename) {
		this.set_filename(filename);
	}
	
	@Override
	public void save_observation() throws Exception
	{
		
	}
	
	@Override
	public void save_forecast() throws Exception
	{
		String lead = Utilities.extract_word(get_filename(), "(?<=\\.f)[0-9]+");
		lead = Utilities.extract_word(lead, "[1-9][0-9]*");

		NetcdfFile source = get_source();
		Attribute lead_attribute = source.findGlobalAttributeIgnoreCase("model_output_valid_time");
		String valid_time = lead_attribute.getStringValue().replaceAll("_", " ");
		
		
		String save_script = String.format(save_source_script, 
										   valid_time, 
										   get_absolute_filename(),
										   "'y'",
										   lead,
										   valid_time,
										   get_absolute_filename(),
										   lead);
		Database.execute(save_script);
		
		String load_script = String.format(get_source_id_script,
										   valid_time,
										   get_absolute_filename(),
										   lead);
		Integer source_id = Database.get_result(load_script, "netcdfsource_id");
		System.out.println("Saved netcdf of with source id of: " + String.valueOf(source_id));
	}
	
	private void save_locations() throws IOException
	{
		Variable location_var = get_source().findVariable("station_id");
		if (location_var == null)
		{
			location_var = get_source().findVariable("feature_id");
		}
		
		if (location_var != null)	// Data is stored by comid
		{
			
		}
		else						// Data might be stored by some sort of x/y variables
		{
			Variable x_variable = get_source().findVariable("x");
			if (x_variable != null) 			// There are definitions for the x and y coordinates
			{
				save_location_by_latlon(x_variable);
			}
		}
	}
	
	private void save_variables()
	{
		
	}
	
	private void save_location_by_comid()
	{
		try {
			Variable var = get_source().findVariable("station_id");
			int length = var.getDimension(0).getLength();

            int[] origin = null;
            int[] size = null;
            
			for (int index = 0; index < length; ++length)
			{
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void save_location_by_latlon(Variable x_variable) throws IOException
	{
			Variable y_variable = get_source().findVariable("y");
			
			int x_length = x_variable.getDimension(0).getLength();
			int y_length = y_variable.getDimension(0).getLength();
			
			int insert_count = 0;
			List<List<Object>> parameters = new ArrayList<List<Object>>();
			
			for (int x = 0; x < x_length; ++x)
			{
				for (int y = 0; y < y_length; ++y)
				{
					
				}
			}
	}
	
	private NetcdfFile get_source() throws IOException
	{
		if (source == null)
		{
			source = NetcdfFile.open(get_absolute_filename());
		}
		return source;
	}
	
	private NetcdfFile source = null;

	private List<Integer> comids;
	private List<Double> latitudes;
	private List<Double> longitudes;
	
	private final String get_source_id_script = "SELECT NS.netcdfsource_id\n"
												+ "FROM netcdfsource NS\n"
												+ "WHERE NS.valid_date = '%s'\n"
												+ "		AND NS.file_path = '%s'\n"
												+ "		AND NS.lead_time = %s;";
	
	private final String save_source_script = "INSERT INTO NetCDFSource (\n"
											+ "		valid_date,\n"
											+ "		file_path,\n"
											+ "		is_forecast,\n"
											+ "		lead_time\n"
											+ ")\n"
											+ "SELECT '%s',\n"
											+ "		'%s',\n"
											+ "		%s,\n"
											+ "		%s\n"
											+ "WHERE NOT EXISTS (\n"
											+ "		SELECT 1\n"
											+ "		FROM NetCDFSource"
											+ "		WHERE valid_date = '%s'\n"
											+ "			AND file_path = '%s'\n"
											+ "			AND lead_time = '%s'\n"
											+ ");";
	
	private final String save_by_com_script = "INSERT INTO ObservationLocation (\n"
											  + "	comid,\n"
											  + "	lid,\n"
											  + "	gage_id,\n"
											  + "	st,\n"
											  + "	nws_st\n"
											  + ")\n"
											  + "SELECT ?, ?, ?, ?, ?\n"
											  + "WHERE NOT EXISTS (\n"
											  + "	SELECT 1\n"
											  + "	FROM ObservationLocation L\n"
											  + "	WHERE comid = ?\n"
											  + ");\n"
											  + "INSERT INTO ForecastResult (\n"
											  + "	forecast_id,\n"
											  + "	lead_time,\n"
											  + "	measurements,\n"
											  + "	observationlocation_id\n"
											  + ")\n"
											  + "SELECT ?, ?, ?, ?";
	
	private final String save_add_com_script = "INSERT INTO ObservationLocation (\n"
											  + "	comid,\n"
											  + "	lid,\n"
											  + "	gage_id,\n"
											  + "	st,\n"
											  + "	nws_st\n"
											  + ")\n"
											  + "VALUES (?, ?, ?, ?, ?)";
	
	private final String save_latlon_script = "INSERT INTO Coordinate (\n"
											 + "	geographic_coordinate,\n"
											 + "	resolution\n"
											 + ")\n"
											 + "SELECT ST_Point(?, ?),\n"
											 + "	?\n"
											 + "WHERE NOT EXISTS (\n"
											 + "	SELECT 1\n"
											 + "	FROM Coordinate"
											 + "	WHERE geographic_coordinate = ST_Point(?, ?)\n"
											 + "		AND resolution = ?\n"
											 + ");";
}
