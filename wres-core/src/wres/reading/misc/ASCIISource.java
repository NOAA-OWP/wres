/**
 * 
 */
package wres.reading.misc;

import wres.reading.BasicSource;
import wres.reading.SourceType;
import wres.util.Database;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import wres.concurrency.Executor;
import wres.data.Variable;

/**
 * @author ctubbs
 *
 */
public class ASCIISource extends BasicSource 
{
    private static int MAX_INSERTS = 100;
    private Integer observationlocation_id;
    private String absolute_path;
    private String variable_name;

	public ASCIISource(String filename)
	{
		//TODO: Remove variable_name hardcoding
		set_variable_name("precipitation");
		set_filename(filename);
		set_source_type(SourceType.ASCII);
	}
	
	private Integer get_observationlocation_id() throws SQLException
	{
		if (observationlocation_id == null)
		{
			String location_name = Paths.get(get_filename()).getFileName().toString().split("_")[0];
			String load_script = String.format(get_observationlocation_id_script, location_name);
			ResultSet result = Database.execute_for_result(load_script);
			observationlocation_id = result.getInt("observationlocation_id");
		}
		return observationlocation_id;
	}
	
	private void set_variable_name(String variable_name)
	{
		this.variable_name = variable_name;
	}
	
	private String get_variable_name()
	{
		return variable_name;
	}
	
	private String get_absolute_path()
	{
		if (absolute_path == null)
		{
			absolute_path = Paths.get(get_filename()).toAbsolutePath().toString();
		}
		return absolute_path;
	}

	public void save_forecast() throws SQLException {
		Path path = Paths.get(get_filename());
		
		try(BufferedReader reader = Files.newBufferedReader(path))
		{
			wres.util.Stopwatch stopwatch = new wres.util.Stopwatch();
			stopwatch.start();
			System.out.println("Removing all previous data for this datasource...");
			String absolute_path = path.toAbsolutePath().toString();

			String clear_statement = "DELETE FROM Forecast WHERE source = '%s';";
			clear_statement = String.format(clear_statement, absolute_path, absolute_path);
			Database.execute(clear_statement);
			
			System.out.println("All previous data for this data source has been removed. Now saving forecast... (" + stopwatch.get_formatted_duration() + ")");

			float current_step = 99999999.9f;
			String line = "";

			int forecast_id = 0;
            int insert_count = 0;
            HashMap<Integer, HashMap<String, String[]>> forecasted_values = new HashMap<Integer, HashMap<String, String[]>>();
			HashMap<String, String[]> hourly_values = null;
			while ((line = reader.readLine()) != null)
			{
				String[] ascii = line.split(" ");
				float step = Float.parseFloat(ascii[2]);
				if (current_step > step)
				{									
					if (hourly_values != null)
					{
						forecasted_values.put(forecast_id, hourly_values);
						insert_count++;
						
						if (MAX_INSERTS <= insert_count)
						{
							insert_count = 0;
							Runnable worker = new ASCIIResultSaver(forecasted_values, get_observationlocation_id());
							Executor.execute(worker);
		                    forecasted_values = new HashMap<Integer, HashMap<String, String[]>>();
						}
						
					}
					
					hourly_values = new HashMap<String, String[]>();
					forecast_id = create_forecast(line);
				}
				current_step = step;
				hourly_values.put(ascii[2].replace(".0", ""), Arrays.copyOfRange(ascii, 3, ascii.length));
			}
			
			if (hourly_values.size() > 0)
			{
				forecasted_values.put(forecast_id, hourly_values);
				Runnable worker = new ASCIIResultSaver(forecasted_values, get_observationlocation_id());
				Executor.execute(worker);
			}
			
			Executor.shutdown();
			System.out.println("Lines distributed. Currently saving to the database... (" + stopwatch.get_formatted_duration() + ")");
		}
		catch (IOException exception)
		{
			System.err.format("IOException: %s%n", exception);
		}		
	}
	
	/**
	 * Parses a line of Tabular ASCII, transforms it into a SQL statement and sends it to the database
	 * 
	 * @param line The line of ASCII that needs to be parsed
	 * @return The ID of the new forecast
	 * @throws SQLException A SQLException is thrown if something goes awry when connecting to the database,
	 * sending the query, or interpretting the result
	 */
	private int create_forecast(String line) throws SQLException
	{
		int forecast_id = 0;
		String save_script = "No SQL statement has been written yet.";
		String[] ascii = line.split("\\s+");
		ResultSet results = null;

		try {
			String lead = ascii[2].replace(".0", "");
			Integer numeric_hour = Integer.parseInt(ascii[1]);
			String hour = "00";
			
			if (numeric_hour > 0)
			{
				hour = String.valueOf(numeric_hour - Integer.parseInt(lead));
			}
			
			String formatted_date = ascii[0] + " " + hour + ":00:00.00";


			save_script = get_save_forecast_script(formatted_date);

			results = Database.execute_for_result(save_script);
			
			forecast_id = results.getInt("forecast_id");
		} 
		catch (SQLException error)
		{
			System.out.println("\nA forecast could not be created. Here is the script:\n");
			System.out.println(save_script);
			throw error;
		}
		finally
		{
			if (results != null)
			{
				results.close();
			}
		}
		return forecast_id;
	}
	
	/**
	 * Generates a script to specially save a single forecast to the database. 
	 * @return The script needed to insert a new forecast
	 * 
	 * TODO: Move the script to a separate file so that it is easier to read and may be edited without recompiling
	 */
	private String get_save_forecast_script(String formatted_date)
	{
		String script = save_forecast_script;
		script = String.format(script, 
							   formatted_date, 
							   get_absolute_path(), 
							   Variable.get_variable_id(get_variable_name()));		
		return script;
	}
	
	private final String save_forecast_script = "INSERT INTO Forecast(forecast_date, source, measurementunit_id, variable_id)\n" +
												"VALUES ('%s', '%s', 1, %d)\n" +
												"RETURNING forecast_id;";
	
	private final String get_observationlocation_id_script = "SELECT observationlocation_id\n" +
															 "FROM ObservationLocation\n" +
															 "WHERE location_name = '%s';";
}
