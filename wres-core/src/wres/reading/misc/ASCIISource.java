/**
 * 
 */
package wres.reading.misc;

import wres.reading.BasicSource;
import wres.reading.BasicSeries;
import wres.reading.SourceType;
import wres.util.Database;
import wres.reading.BasicSeriesEntry;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import wres.concurrency.Executor;

/**
 * @author ctubbs
 *
 */
public class ASCIISource extends BasicSource 
{

    private static int THREAD_COUNT = 100;
    private static int MAX_INSERTS = 100;

	public ASCIISource(String filename)
	{
		set_filename(filename);
		set_source_type(SourceType.ASCII);
	}
	
	private String next_member_id()
	{
		return String.valueOf(time_series.size());
	}
	
	public static void write(BasicSource source, String path)
	{
		File file = new File(path);
		
		try {
			file.createNewFile();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		try {
			BufferedWriter writer = null;
			String line = "";
			DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH");
			Iterator<BasicSeries> time_series = source.get_series();
			BasicSeries series = null;
			while (time_series.hasNext())
			{
				series = time_series.next();
				
				if (writer == null)
				{
					writer = new BufferedWriter(new FileWriter(file));
				}
				
				Iterator<BasicSeriesEntry> entry_iterator = series.get_entries();
				
				while (entry_iterator.hasNext())
				{
					BasicSeriesEntry entry = entry_iterator.next();
					line = formatter.format(entry.date);
					line += " ";
					line += String.valueOf(entry.lead_time);
					
					for (Double value : entry.values)
					{
						line += " ";
						line += String.valueOf(value);
					}
					
					line += System.lineSeparator();
					writer.write(line);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void write(String path) {
		write(this, path);
		
	}
	
	@Override
	public Date get_forecast_date()
	{
		Date forecasted_date = super.get_forecast_date();
		if (time_series.size() > 0)
		{
			forecasted_date = time_series.get(0).get_forecast_date();
		}
		return forecasted_date;
	}
	
	@Override
	public void read() 
	{
		Path path = Paths.get(get_filename());
		double current_lead_time = -1.0;
		ASCIISeries series = new ASCIISeries();
		try(BufferedReader reader = Files.newBufferedReader(path))
		{
			String line = null;
			String[] ascii;
			ASCIIEntry entry;

			DateFormat formatter = new SimpleDateFormat("MM/dd/yyyy HH");
			while ((line = reader.readLine()) != null)
			{
				entry = new ASCIIEntry();
				ascii = line.split(" ");

				entry.date = formatter.parse(ascii[0] + " " + ascii[1]);				
				
				if (series.get_ensemble_member_id().isEmpty())
				{
					series.set_ensemble_member_id(next_member_id());
				}
				
				entry.lead_time = Double.parseDouble(ascii[2]);
				
				for (int i = 3; i < ascii.length; ++i)
				{
					entry.values.add(Double.parseDouble(ascii[i]));
				}
				
				if (series.length() > 0 && entry.lead_time <= current_lead_time)
				{
					time_series.add(series);
					current_lead_time = 0;
					series = new ASCIISeries();
				}
				else if (series.length() > 0 && entry.lead_time > current_lead_time)
				{
					series.set_aggregation_period(entry.lead_time - current_lead_time);
				}

				current_lead_time = entry.lead_time;
				series.add_entry(entry);
			}			
			time_series.add(series);
		}
		catch (IOException exception)
		{
			System.err.format("IOException: %s%n", exception);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
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
			
			// TODO: Implement Global Thread Pool
			//ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

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
							Runnable worker = new ASCIIEntryParser(forecasted_values);
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
				Runnable worker = new ASCIIEntryParser(forecasted_values);
				Executor.execute(worker);
			}
			
			System.out.println("All threads used to create insert statements have been created... (" + stopwatch.get_formatted_duration() + ")");
			Executor.shutdown();
			/*while (!executor.isTerminated())
			{
			}*/
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
		String save_script = get_save_forecast_script();
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


			save_script = String.format(save_script, formatted_date, lead);

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
	private String get_save_forecast_script()
	{
		String script = "INSERT INTO Forecast (forecast_date, source, measurementunit_id, variable_id)";
		script += System.lineSeparator();
		// TODO: The following static values need to be evaluated rather than hardcoded
		script += "SELECT '%s',";
		script += System.lineSeparator();
		script += "'";
		script += Paths.get(get_filename()).toAbsolutePath().toString();
		script += "',";
		script += System.lineSeparator();
		script += "1,";
		script += System.lineSeparator();
		script += "V.variable_id";
		script += System.lineSeparator();
		script += "FROM Variable V";
		script += System.lineSeparator();
		script += "WHERE V.variable_name = 'precipitation'";
		script += System.lineSeparator();
		script += "RETURNING forecast_id;";
		
		return script;
	}

	// TODO: Implement the process of saving Tabular ASCII as observations
	@Override
	public void save_observation() throws SQLException {
		System.err.println("Tabular ASCII source files don't currently have the ability to save as observation data.");
	}

}
