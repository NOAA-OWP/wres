/**
 * 
 */
package wres.util;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import org.postgresql.Driver;
import wres.reading.BasicSource;
import wres.reading.SourceReader;
/**
 * @author ctubbs
 *
 */
public class MainFunctions {

	private static final Map<String, Consumer<String[]>> functions = createMap();
	
	public static final boolean has_operation(String operation)
	{
		return functions.containsKey(operation.toLowerCase());
	}
	
	public static final void call(String operation, String[] args)
	{
		operation = operation.toLowerCase();
		functions.get(operation).accept(args);
	}
	
	private static final Map<String, Consumer<String[]>> createMap()
	{
		Map<String, Consumer<String[]>> prototypes = new HashMap<String, Consumer<String[]>>();
		
		prototypes.put("loadwaterdata", loadWaterData());
		prototypes.put("copywaterdata", copyWaterData());
		prototypes.put("describenetcdf", describeNetCDF());
		prototypes.put("connecttodb", connectToDB());
		prototypes.put("saveforecast", saveForecast());
		prototypes.put("saveobservation", saveObservation());
		prototypes.put("printpairs", printPairs());
		
		return prototypes;
	}
	
	private static final Consumer<String[]> loadWaterData()
	{
		return (String[] args) ->
		{
			if (args.length > 0)
			{
				try {
					BasicSource source = SourceReader.get_source(args[0]);
					source.read();
					source.print();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path is needed to load data. Please pass that in as the first argument.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}

	private static final Consumer<String[]> copyWaterData()
	{
		return (String[] args) ->{
			if (args.length > 0)
			{	String filename = args[0].replaceFirst("\\.", "_copy.");
				try {
					BasicSource source = SourceReader.get_source(args[0]);
					source.read();
					source.write(filename);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path is needed to copy data. Please pass that in as the first argument.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}
	
	private static final Consumer<String[]> saveForecast()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				try
				{
					BasicSource source = SourceReader.get_source(args[0]);
					System.out.println(String.format("Attempting to save '%s' to the database...", args[0]));
					source.save_forecast();
					System.out.println("Database save operation completed. Please verify data.");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path is needed to save data. Please pass that in as the first argument.");
				System.out.println("For now, ensure that the path points towards a tabular ASCII file.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}
	
	private static final Consumer<String[]> saveObservation()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				try
				{
					BasicSource source = SourceReader.get_source(args[0]);
					System.out.println(String.format("Attempting to save '%s' to the database...", args[0]));
					source.save_observation();
					System.out.println("Database save operation completed. Please verify data.");
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
			else
			{
				System.out.println("A path is needed to save data. Please pass that in as the first argument.");
				System.out.println("For now, ensure that the path points towards a datacard file.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}
	
	private static final Consumer<String[]> connectToDB()
	{
		return (String[] args) -> {
			try {
				Properties props = new Properties();
				props.setProperty("user", wres.util.Utilities.DATABASE_USERNAME);
				props.setProperty("password", wres.util.Utilities.DATABASE_PASSWORD);
				Driver d = new Driver();
				Connection conn = d.connect(wres.util.Utilities.DATABASE_URL, props);
				System.out.println("Successfully connected to the database");
				conn.close();
			} catch (SQLException e) {
				System.out.println("Could not connect to database because:");
				e.printStackTrace();
			}
		};
	}
	
	private static final Consumer<String[]> printPairs()
	{
		return (String[] args) -> {
			System.out.println("'printPairs' is an incomplete function and therefore cannot be called yet. Please try again after implementation.");
			/*if (args.length > 2)
			{
				String variable = args[0];
				String location = args[1];
				
				StringBuilder script_builder = new StringBuilder();
				script_builder.append("WITH forecast_measurements AS (");
				script_builder.append(System.lineSeparator());
				script_builder.append("SELECT DISTINCT forecast_id, lead_time, array_agg(FR.measurement) OVER (PARTITION BY forecast_id, lead_time) AS measurements");
				script_builder.append(System.lineSeparator());
				script_builder.append("FROM ForecastResult FR");
				script_builder.append(System.lineSeparator());
				script_builder.append("ORDER BY lead_time");
				script_builder.append(System.lineSeparator());
				script_builder.append(")");
				script_builder.append(System.lineSeparator());
				script_builder.append("SELECT (F.forecast_date + (INTERVAL '1 hour' * FM.lead_time)) AS forecast_date, FM.lead_time, O.measurement, FM.measurements");
				script_builder.append(System.lineSeparator());
				script_builder.append("FROM Forecast F");
				script_builder.append(System.lineSeparator());
				script_builder.append("INNER JOIN forecast_measurements FM");
				script_builder.append(System.lineSeparator());
				script_builder.append("ON FM.forecast_id = F.forecast_id");
				script_builder.append(System.lineSeparator());
				script_builder.append("INNER JOIN ObservationResult O");
				script_builder.append(System.lineSeparator());
				script_builder.append("ON O.valid_date = (F.forecast_date + (INTERVAL '1 hour' * FM.lead_time))");
				script_builder.append(System.lineSeparator());
				script_builder.append("AND O.variable_id = F.variable_id");
				script_builder.append(System.lineSeparator());
				script_builder.append("AND O.observationlocation_id = F.observationlocation_id");
				script_builder.append(System.lineSeparator());
				script_builder.append("INNER JOIN Variable V");
				script_builder.append(System.lineSeparator());
				
				/*WITH forecast_measurements AS (
						SELECT DISTINCT forecast_id, lead_time, array_agg(FR.measurement) OVER (PARTITION BY forecast_id, lead_time) AS measurements
						FROM ForecastResult FR
						ORDER BY lead_time
					)
					SELECT (F.forecast_date + (INTERVAL '1 hour' * FM.lead_time)) AS forecast_date, FM.lead_time, O.measurement, FM.measurements
					FROM Forecast F
					INNER JOIN forecast_measurements FM
						ON FM.forecast_id = F.forecast_id
					INNER JOIN ObservationResult O
						ON O.valid_date = (F.forecast_date + (INTERVAL '1 hour' * FM.lead_time))
					INNER JOIN Observation OBS
						ON O.observation_id = OBS.observation_id
							AND OBS.variable_id = F.variable_id
							AND OBS.observationlocation_id = F.observationlocation_id
					INNER JOIN ObservationLocation OL
						ON OL.observationlocation_id = OBS.observationlocation_id
					INNER JOIN Variable V
						ON V.variable_id = OBS.variable_id
					WHERE V.variable_name = variable
						AND OL.location_name = location
						
					ORDER BY F.forecast_date, lead_time
			}
			else
			{
				System.out.println("Not enough arguments were passed.");
				System.out.println("Prototype.jar printpairs <location name> <variable name>");
			}*/
		};
	}
	
	private static final Consumer<String[]> describeNetCDF()
	{
		return (String[] args) -> {
			if (args.length > 0)
			{
				NetCDFReader reader = new NetCDFReader(args[0]);
				reader.output_variables();
			}
			else
			{
				System.out.println("A path is needed to describe the data. Please pass that in as the first argument.");
				System.out.print("The current directory is:\t");
				System.out.println(System.getProperty("user.dir"));
			}
		};
	}
}
