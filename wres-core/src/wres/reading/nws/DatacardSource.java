/**
 * 
 */
package wres.reading.nws;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import wres.reading.BasicSource;
import wres.reading.SourceType;

/**
 * @author ctubbs
 *
 */
public class DatacardSource extends BasicSource {

    private static int THREAD_COUNT = 50;
    private static int MAX_INSERTS = 100;
	/**
	 * 
	 */
	public DatacardSource(String filename) {
		set_filename(filename);
		set_source_type(SourceType.DATACARD);
	}

	@Override
	public void read() throws Exception {
		Path path = Paths.get(get_filename());
		
		try (BufferedReader reader = Files.newBufferedReader(path))
		{
			String line = null;
			
			int line_number = 1;
			Pattern missing_data_pattern = Pattern.compile("(?<=SYMBOL FOR MISSING DATA=)[-\\.\\d]*");
			Pattern accumulated_data_pattern = Pattern.compile("(?<=SYMBOL FOR ACCUMULATED DATA=)[-\\.\\d]*");
			while (line_number < 6 && (line = reader.readLine()) != null)
			{
				Matcher missing_data_matcher = missing_data_pattern.matcher(line);
				if (missing_data_matcher.find())
				{
					missing_data_symbol = missing_data_matcher.group();
				}

				Matcher accumulated_data_matcher = accumulated_data_pattern.matcher(line);
				if (accumulated_data_matcher.find())
				{
					accumulated_data_symbol = accumulated_data_matcher.group();
				}
				line_number++;
			}
			
			if ((line = reader.readLine()) != null)
			{
				set_datatype_code(line.substring(14, 18));
				set_data_dimensions_code(line.substring(19, 23));
				set_data_units_code(line.substring(24, 28));
				set_time_interval(line.substring(29, 31));
				set_time_series_identifier(line.substring(34, 46));
				set_series_description(line.substring(49, 69));
			}
			else
			{
				String message = "The NWS Datacard file ('%s') was not formatted correctly and could not be loaded correctly";
				throw new Exception(String.format(message, get_filename()));
			}
			
			if ((line = reader.readLine()) != null)
			{
				set_first_month(line.substring(0, 2));
				set_first_year(line.substring(4, 8));
				set_last_month(line.substring(9, 12));
				set_last_year(line.substring(14, 18));
				set_values_per_record(line.substring(19, 21));
				set_data_format(line.substring(24, 32));
			}
			else
			{
				String message = "The NWS Datacard file ('%s') was not formatted correctly and could not be loaded correctly";
				throw new Exception(String.format(message, get_filename()));
			}					
						
			DatacardSeries series = new DatacardSeries();
			DateFormat formatter = new SimpleDateFormat("MM/yyyy");
			String start_date = String.format("%d/%d",  get_first_month(), get_first_year());
			String end_date = String.format("%d/%d", get_last_month(), get_last_year());
			series.set_start_date(formatter.parse(start_date));
			series.set_forecast_date(series.get_start_date());
			set_forecast_date(series.get_forecast_date());
			series.set_end_date(formatter.parse(end_date));
			
			int current_lead = 0;
			
			while ((line = reader.readLine()) != null)
			{
				String identifier = line.substring(0,  12);
				String sequence_number = line.substring(16, 20);
				String[] values = line.substring(20).trim().split("\\s+");
				
				for (int row_index = 0; row_index < values.length; ++row_index)
				{
					DatacardEntry entry = new DatacardEntry();
					entry.add_value(values[row_index]);
					entry.lead_time = current_lead * get_time_interval();
					entry.set_sequence_number(sequence_number);
					Calendar calendar = Calendar.getInstance();
					calendar.setTime(series.get_forecast_date());
					calendar.add(Calendar.HOUR, (int) entry.lead_time);
					entry.date = calendar.getTime();
					entry.set_series_identifier(identifier);
					series.add_entry(entry);
					current_lead++;
				}
			}
			time_series.add(series);
		}
		catch (IOException exception)
		{
			System.err.format("IOException: %s%n", exception);
			throw exception;
		}
		
	}
	
	@Override
	public void write(String path) {
		// TODO Auto-generated method stub
		
	}
	
	public String get_datatype_code()
	{
		return datatype_code;
	}
	
	public void set_datatype_code(String code)
	{
		datatype_code = code.trim();
	}
	
	public String get_data_dimensions_code()
	{
		return data_dimensions_code;
	}
	
	public void set_data_dimensions_code(String code)
	{
		data_dimensions_code = code.trim();
	}
	
	public String get_data_units_code()
	{
		return data_units_code;
	}
	
	public void set_data_units_code(String code)
	{
		data_units_code = code.trim();
	}
	
	public int get_time_interval()
	{
		return time_interval;
	}
	
	public void set_time_interval(int interval)
	{
		time_interval = interval;
	}
	
	public void set_time_interval(String interval)
	{
		interval = interval.trim();
		set_time_interval(Integer.parseInt(interval));
	}
	
	public String get_series_description()
	{
		return series_description;
	}
	
	public void set_series_description(String description)
	{
		series_description = description.trim();
	}
	
	public int get_first_month()
	{
		return first_month;
	}
	
	public void set_first_month(int month)
	{
		first_month = month;
	}
	
	public void set_first_month(String month_number)
	{
		month_number = month_number.trim();
		set_first_month(Integer.parseInt(month_number));
	}
	
	public int get_first_year()
	{
		return first_year;
	}
	
	public void set_first_year(int year)
	{
		first_year = year;
	}
	
	public void set_first_year(String year)
	{
		year = year.trim();
		set_first_year(Integer.parseInt(year));
	}
	
	public int get_last_month()
	{
		return last_month;
	}
	
	public void set_last_month(int month)
	{
		last_month = month;
	}
	
	public void set_last_month(String month_number)
	{
		month_number = month_number.trim();
		set_last_month(Integer.parseInt(month_number));
	}
	
	public int get_last_year()
	{
		return last_year;
	}
	
	public void set_last_year(int year)
	{
		last_year = year;
	}
	
	public void set_last_year(String year)
	{
		year = year.trim();
		set_last_year(Integer.parseInt(year));
	}
	
	public int get_values_per_record()
	{
		return values_per_record;
	}
	
	public void set_values_per_record(int amount)
	{
		values_per_record = amount;
	}
	
	public void set_values_per_record(String amount)
	{
		amount = amount.trim();
		set_values_per_record(Integer.parseInt(amount));
	}
	
	public String get_data_format()
	{
		return data_format;
	}
	
	public void set_data_format(String format)
	{
		data_format = format.trim();
	}	
	
	public String get_missing_data_symbol()
	{
		return missing_data_symbol;
	}
	
	public void set_missing_data_symbol(String symbol)
	{
		missing_data_symbol = symbol;
	}
	
	public String get_accumulated_data_symbol(String symbol)
	{
		return accumulated_data_symbol;
	}
	
	public void set_accumulated_data_symbol(String symbol)
	{
		accumulated_data_symbol = symbol.trim();
	}
	
	public String get_time_series_identifier()
	{
		return time_series_identifier;
	}
	
	public void set_time_series_identifier(String identifier)
	{
		time_series_identifier = identifier.trim();
	}
	
	@Override
	public Date get_forecast_date()
	{
		Calendar cal = Calendar.getInstance();
		cal.set(get_first_year(), get_first_month(), 1);
		return cal.getTime();
	}
		
	private String datatype_code = "";
	private String data_dimensions_code = "";
	private String data_units_code = "";
	private int time_interval = 0;
	private String time_series_identifier = "";
	private String series_description = "";
	private int first_month = 0;
	private int first_year = 0;
	private int last_month = 0;
	private int last_year = 0;
	private int values_per_record = 0;
	private String data_format = "";
	private String missing_data_symbol = "-999.00";
	private String accumulated_data_symbol = "-998.00";
	@Override
	public void save_forecast() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void save_observation() throws Exception {
		Path path = Paths.get(get_filename());
		
		try (BufferedReader reader = Files.newBufferedReader(path))
		{
			String line = null;
			
			int line_number = 1;
			Pattern missing_data_pattern = Pattern.compile("(?<=SYMBOL FOR MISSING DATA=)[-\\.\\d]*");
			Pattern accumulated_data_pattern = Pattern.compile("(?<=SYMBOL FOR ACCUMULATED DATA=)[-\\.\\d]*");
			while (line_number < 6 && (line = reader.readLine()) != null)
			{
				Matcher missing_data_matcher = missing_data_pattern.matcher(line);
				if (missing_data_matcher.find())
				{
					missing_data_symbol = missing_data_matcher.group();
				}

				Matcher accumulated_data_matcher = accumulated_data_pattern.matcher(line);
				if (accumulated_data_matcher.find())
				{
					accumulated_data_symbol = accumulated_data_matcher.group();
				}
				line_number++;
			}
			
			if ((line = reader.readLine()) != null)
			{
				set_datatype_code(line.substring(14, 18));
				set_data_dimensions_code(line.substring(19, 23));
				set_data_units_code(line.substring(24, 28));
				set_time_interval(line.substring(29, 31));
				set_time_series_identifier(line.substring(34, 46));
				set_series_description(line.substring(49, 69));
			}
			else
			{
				String message = "The NWS Datacard file ('%s') was not formatted correctly and could not be loaded correctly";
				throw new Exception(String.format(message, get_filename()));
			}
			
			if ((line = reader.readLine()) != null)
			{
				set_first_month(line.substring(0, 2));
				set_first_year(line.substring(4, 8));
				set_last_month(line.substring(9, 12));
				set_last_year(line.substring(14, 18));
				set_values_per_record(line.substring(19, 21));
				set_data_format(line.substring(24, 32));
			}
			else
			{
				String message = "The NWS Datacard file ('%s') was not formatted correctly and could not be loaded correctly";
				throw new Exception(String.format(message, get_filename()));
			}					
			
			String observation_id = create_observation();
			
			OffsetDateTime datetime = OffsetDateTime.of(get_first_year(), get_first_month(), 1, 0, 0, 0, 0, ZoneOffset.UTC);

			int current_lead = 6;

			ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
			HashMap<OffsetDateTime, String> dated_values = new HashMap<OffsetDateTime, String>();
			int entry_count = 0;
			while ((line = reader.readLine()) != null)
			{
				String[] values = line.substring(20).trim().split("\\s+");
				
				for (int row_index = 0; row_index < values.length; ++row_index)
				{
					if (!values[row_index].startsWith(missing_data_symbol) && !values[row_index].startsWith(accumulated_data_symbol))
					{
						dated_values.put(datetime.plusHours((long)current_lead), values[row_index]);
						entry_count++;
					}
					
					if (entry_count >= MAX_INSERTS)
					{
						Runnable worker = new DatacardEntryParser(observation_id, dated_values);
						executor.execute(worker);
						dated_values = new HashMap<OffsetDateTime, String>();
						entry_count = 0;
					}
					
					current_lead += get_time_interval();
				}
			}
			
			if (entry_count > 0)
			{
				Runnable worker = new DatacardEntryParser(observation_id, dated_values);
				executor.execute(worker);
			}
			
			System.out.println("Lines distributed. Currently saving to the database...");
			executor.shutdown();
			while (!executor.isTerminated())
			{
			}
		}
		catch (IOException exception)
		{
			System.err.format("IOException: %s%n", exception);
			throw exception;
		}		
	}
	
	private String create_observation() throws SQLException
	{
		int observation_id = 0;
		Connection connection = null;
		ResultSet results = null;
		String save_script = get_save_observation_script();
		try {
			connection = wres.util.Utilities.create_eds_connection();
			Statement query = connection.createStatement();
			if (query.execute(save_script))
			{
				results = query.getResultSet();
				results.next();
				observation_id = results.getInt("observation_id");
			}
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
			
			if (connection != null)
			{
				connection.close();
			}
		}
		return String.valueOf(observation_id);
	}
	
	private String get_save_observation_script()
	{
		Path path = Paths.get(get_filename());
		path = path.getFileName();
		String location_name = path.toString().replace(".MAP06", "");
		String script = "INSERT INTO Observation (source, observationlocation_id, variable_id, measurementunit_id, projection_id)";
		script += System.lineSeparator();
		script += "SELECT '";
		script += get_filename();
		script += "',";
		script += System.lineSeparator();
		script += "loc.observationlocation_id,";
		script += System.lineSeparator();
		script += "v.variable_id,";
		script += System.lineSeparator();
		script += "1,";
		script += System.lineSeparator();
		script += "1";
		script += System.lineSeparator();
		script += "FROM ObservationLocation loc";
		script += System.lineSeparator();
		script += "CROSS JOIN Variable V";
		script += System.lineSeparator();
		script += "WHERE loc.lid = '";
		script += location_name;
		script += "'";
		script += System.lineSeparator();
		script += "AND v.variable_name = 'precipitation'";
		script += System.lineSeparator();
		script += "RETURNING observation_id;";
		return script;
	}
}
