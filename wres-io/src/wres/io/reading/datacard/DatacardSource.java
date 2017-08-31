package wres.io.reading.datacard;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.io.concurrency.CopyExecutor;
import wres.io.data.caching.DataSources;
//import concurrency.DatacardResultSaver;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.utilities.Database;
import wres.util.ProgressMonitor;
import wres.util.Time;

/**
 * @author ctubbs
 *
 */
@SuppressWarnings("deprecation")
public class DatacardSource extends BasicSource {

    private static final int MAX_INSERTS = 100;
    
	/**
	 * 
	 * @param filename the file name
	 */
	public DatacardSource(String filename) {
		// TODO: Remove hard coding for variable name
		set_variable_name("precipitation");
		setFilename(filename);
		//set_source_type(SourceType.DATACARD);
	}
	
	public String get_datatype_code()
	{
		return datatype_code;
	}
	
	public void set_datatype_code(String code)
	{
		datatype_code = code.trim();
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
	
	public void set_values_per_record(String amount)
	{
		amount = amount.trim();
		values_per_record = Integer.parseInt(amount);
	}
	
	public String get_missing_data_symbol()
	{
		return missing_data_symbol;
	}
	
	public void set_missing_data_symbol(String symbol)
	{
		missing_data_symbol = symbol;
	}
	
	public String get_accumulated_data_symbol()
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
	
	
	private Integer get_variable_id() throws SQLException
	{
		return Variables.getVariableID(get_variable_name(), get_measurement_unit());
	}
	
	public String get_measurement_unit()
	{
		return measuremnt_unit;
	}
	
	public void set_measurement_unit(String unit)
	{
		measuremnt_unit = unit;
	}
	
	public int get_measurement_unit_id()
	{
		int id = -1;
		
		try 
		{
			id = MeasurementUnits.getMeasurementUnitID(measuremnt_unit);
		}
		catch(SQLException ex)
		{
			ex.printStackTrace();
		}
		
		return id;
	}

	@Override
	public void saveObservation() throws IOException {
		Path path = Paths.get(getFilename());
		
		currentVariableName = getSpecifiedVariableName();
		measuremnt_unit     = getSpecifiedVariableUnit();
										
		try (BufferedReader reader = Files.newBufferedReader(path))
		{
			String line           = null;
			int    obsValColWidth = 0;
			int	   lastColIdx     = 0;
			
			//TODO Hank (8/31/17): You cannot rely on the $ lines to exist.  Furhter, since they are comment lines,
			//you cannot depend on them for information.  Instead, the missing data value will need to be provided
            //through configuration.  And, since multiple values can serve as missing (-998 should be treated as -999,
			//both being "missing"), you may need to allow for multiple possible values to denote missing.
			//
			//In any case, if a user does not specify missing, then it should assume -998 and -999 are both
			//missing.  I think this could be handled in the config side by supplying a default for whatever 
			//element is used for the user to specify the missing value.
			Pattern missing_data_pattern     = Pattern.compile("(?<=SYMBOL FOR MISSING DATA=)[-\\.\\d]*");
			Pattern accumulated_data_pattern = Pattern.compile("(?<=SYMBOL FOR ACCUMULATED DATA=)[-\\.\\d]*");

			//XXX Hank (8/31/17) Process the comment lines.  
			while ((line = reader.readLine()) != null && line.startsWith("$"))
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
			}
			
			//XXX Hank (8/31/17): First non-comment, header line.
			if (line != null)
			{
				set_datatype_code(line.substring(14, 18));
				set_time_interval(line.substring(29, 31));
				set_time_series_identifier(line.substring(34, 46));
				lastColIdx = Math.min(69, line.length() - 1);
				
				if(lastColIdx > 49)
				{
					set_series_description(line.substring(49, lastColIdx));
				}
			}
			else
			{
				String message = "The NWS Datacard file ('%s') was not formatted correctly and could not be loaded correctly";
				throw new IOException(String.format(message, getFilename()));
			}
			
			//XXX Hank (8/31/17): Second non-comment, header line.
			if ((line = reader.readLine()) != null)
			{
				set_first_month(line.substring(0, 2));
				set_first_year(line.substring(4, 8));
				set_last_month(line.substring(9, 11));
				set_last_year(line.substring(14, 18));
				set_values_per_record(line.substring(19, 21));
				lastColIdx = Math.min(32, line.length() - 1);
				
				if(lastColIdx > 24)
				{
					obsValColWidth = getValColWidth(line.substring(24, lastColIdx));
				}
			}
			else
			{
				String message = "The NWS Datacard file ('%s') was not formatted correctly and could not be loaded correctly";
				throw new IOException(String.format(message, getFilename()));
			}					
			
			OffsetDateTime datetime       = OffsetDateTime.of(get_first_year(), get_first_month(), 1, 0, 0, 0, 0, ZoneOffset.UTC);
			String         timeZone       = getSpecifiedTimeZone();
			int            valIdxInRecord = 0;
			String         value          = "";
			
			datetime = datetime.plusHours(Time.zoneOffsetHours(timeZone));
            
			//XXX Hank (8/31/17): Process the lines one at a time.
			while ((line = reader.readLine()) != null)
			{
				for (valIdxInRecord = 0; valIdxInRecord < values_per_record; valIdxInRecord++)
				{
					entry_count++;
					
					//TODO I don't see any logic in here to handle lines with fewer numbers than values_per_record.
					//For example:
					//
                    //		            1048  19     .000     .000     .000     .000     .000     .048
                    //		            1048  20     .038     .108     .093     .001     .000     .001
                    //		            1048  21     .000     .000     .000     .000
					//the last line has only four entries.  The loop above will assume six.  I don't see
					//anything here that will identify that there are no more entries (I think the substring
					//below will fail somehow, but you keep using it as normal).

					//last value in the row/record?
					if(valIdxInRecord == values_per_record - 1)
					{
						value = line.substring(FIRST_OBS_VALUE_START_POS + valIdxInRecord * obsValColWidth);
					}
					else
					{
						value = line.substring(FIRST_OBS_VALUE_START_POS + valIdxInRecord * obsValColWidth,
										   	   FIRST_OBS_VALUE_START_POS + (valIdxInRecord + 1)* obsValColWidth+ 1);
					}
					
					datetime = datetime.plusHours(time_interval);
					
					//TODO Hank (8/31/17): Again, the missing and accumulated data values are not being handled well.
					//First, you should do a numerical check, since, with this check, if missing is "-999.0" but the
					//datacard included a field with "-999", which can happen if someone manually edits the file, you 
					//will not know it is missing.  You should get float values for missing, convert the ingested 
					//value to a float and compare.  
					//
					//Also, please confirm this is necessary.  What if missing values were just forwarded to the database?
					//Would that be okay?  Would the database handle it?
					if (!value.startsWith(missing_data_symbol) && !value.startsWith(accumulated_data_symbol))
					{
						try
						{
							addObservedEvent(datetime.toString(), Float.parseFloat(value));
						}
						catch (Exception e) 
						{
							e.printStackTrace();
	                    }
					}
				}
			}
			
			saveLeftoverObservations();
		}
		catch (IOException exception)
		{
			System.err.format("IOException: %s%n", exception);
			throw exception;
		}
		catch (Exception exception)
		{
			System.err.format("Exception: %s%n", exception);
			throw exception;
		}	
		finally
		{
			 if (LOGGER.isInfoEnabled())
	         {
				 LOGGER.info(String.valueOf(entry_count) + " values of datacardsource saved to database");
	         }
		}
	}
	
	private String createObservation() throws SQLException
	{
		int observation_id = 0;
		String save_script = get_save_observation_script();
		
		try {
			clearStaleObservations();
			observation_id = Database.getResult(save_script, "observation_id");
		}
		catch (SQLException error)
		{
			System.out.println("\nA forecast could not be created. Here is the script:\n");
			System.out.println(save_script);
			throw error;
		}

		return String.valueOf(observation_id);
	}
	
	private void clearStaleObservations() throws SQLException
	{
		String clear_script = "DELETE FROM Observation WHERE source = '" + getAbsoluteFilename() + "';";
		Database.execute(clear_script);
	}
	
	/**
	 * Returns a specialized script used to save the observation
	 * @return The script to save the observation
	 * @throws SQLException
	 */
	private String get_save_observation_script() throws SQLException
	{
		//TODO: Stop hard coding the measurement unit id
		return String.format(save_observation_script, 
							 getAbsoluteFilename(),
							 get_variable_id(),
							 1);
	}
	
	private void set_variable_name(String variable_name)
	{
		this.variable_name = variable_name;
	}
	
	private String get_variable_name() 
	{
		return variable_name;
	}
	
	//TODO Hank (8/31/17): I think this method could check a flag called "testMode" or something similar.  If testing,
	//it could output the currentScript to a file or something else.  You can then check the resulting script
	//with a benchmark to see if its what is expected.  
	private void saveLeftoverObservations()
    {
        if (insertCount > 0)
        {
            insertCount = 0;
            CopyExecutor copier = new CopyExecutor(currentTableDefinition, currentScript.toString(), delimiter);
            copier.setOnRun(ProgressMonitor.onThreadStartHandler());
            copier.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            Database.storeIngestTask(Database.execute(copier));
            currentScript = null;
        }
    }
	
	/**
	 * Adds measurement information to the current insert script in the form of observation data
	 * @param observedTime The time when the measurement was taken
	 * @param observedValue The value retrieved from the XML
	 * @throws Exception Any possible error encountered while trying to retrieve the variable position id or the id of the measurement uni
	 */
	private void addObservedEvent(String observedTime, Float observedValue) throws Exception
	{
        if (!dateIsApproved(observedTime) || !valueIsApproved(observedValue))
        {
            return;
        }
        
		if (insertCount > 0)
		{
			currentScript.append(NEWLINE);
		}
		else
		{
			currentTableDefinition = INSERT_OBSERVATION_HEADER;
			currentScript = new StringBuilder();
		}
		
		currentScript.append(Features.getVariablePositionID(getSpecifiedLocationID(), getSpecifiedLocationID(), getVariableID()));
		currentScript.append(delimiter);
		currentScript.append("'").append(observedTime).append("'");
		currentScript.append(delimiter);
		currentScript.append(observedValue);
		currentScript.append(delimiter);
		currentScript.append(String.valueOf(getMeasurementID()));
		currentScript.append(delimiter);
		currentScript.append(String.valueOf(getSourceID()));
		
		insertCount++;
	}
			
	private Integer getMeasurementID()  throws Exception
	{
		if(currentMeasurementUnitID == null)
		{
			currentMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(getSpecifiedVariableUnit());
		}
		
		return currentMeasurementUnitID ;
	}
	
	/**
	 * @return The ID of the variable currently being measured
	 * @throws Exception Thrown if interaction with the database failed.
	 */
	private int getVariableID() throws Exception
	{		
		if (currentVariableID == null)
		{
			this.currentVariableID = Variables.getVariableID(currentVariableName, measuremnt_unit);
		}
		
		return this.currentVariableID;
	}
	
	/**
	 * @return A valid ID for the source of this PIXML file from the database
	 * @throws Exception Thrown if an ID could not be retrieved from the database
	 */
	private int getSourceID() throws Exception
	{
		if (currentSourceID == null)
		{
			if (this.creationDateTime == null)
			{
				this.creationDateTime = getFileCreationDateTime();
			}
						
			currentSourceID = DataSources.getSourceID(getFilename(), this.creationDateTime, null);
		}
		
		return currentSourceID;
	}
	
	private boolean dateIsApproved(String date)
	{
	    if (!detailsSpecified || (this.specifiedEarliestDate == null && this.specifiedLatestDate == null))
	    {
	        return true;
	    }
	    
	    OffsetDateTime dateToApprove = Time.convertStringToDate(date);
	    
	    return dateToApprove.isAfter(specifiedEarliestDate) && dateToApprove.isBefore(specifiedLatestDate);
	}
	
	private boolean valueIsApproved(Float value)
	{
	    return !value.equals(this.currentMissingValue) &&
	           value >= this.specifiedMinimumValue && 
	           value <= this.specifiedMaximumValue; 
	}
	
	public void setSpecifiedEarliestDate(String earliestDate)
	{
	    this.specifiedEarliestDate = Time.convertStringToDate(earliestDate);
	    this.detailsSpecified = true;
	}
	
	public void setSpecifiedLatestDate(String latestDate)
	{
	    this.specifiedLatestDate = Time.convertStringToDate(latestDate);
	    this.detailsSpecified = true;
	}
	
	/**
	 * Returns number of observation values in file
	 * @return number of observation values in file
	 */
	public int getEntryCount()
	{
	   return entry_count;
	}
	
	/**
	 * Returns number of observation values sent to database
	 * @return number of observation values sent to database
	 */
	public int getInsertCount()
	{
	   return insertCount;
	}
	
	/**
	 * Returns insert query sent to database
	 * @return insert query sent to database
	 */
	public String getInsertQuery()
	{
		return currentScript.toString();
	}
	
	/**
	 * Return the number of columns of allocated for an observation value. In general, it is smaller than 
	 * the number of columns actually used by an observation value
	 * @param formatStr The float output format in FORTRAN 
	 * @return Number of columns 
	 */
	private int getValColWidth(String formatStr)
	{
		int width = 0;
		int idxF  = 0;
		int idxPeriod = 0;
		
		if(formatStr != null && formatStr.length() > 3)
		{
			idxF = formatStr.toUpperCase().indexOf('F');
			idxPeriod = formatStr.indexOf('.');
		}
		
		if(idxPeriod > idxF)
		{
			width = Integer.parseInt(formatStr.substring(idxF + 1, idxPeriod));
		}
		
		return width;
	}
	
	/**
	 * Returns a string of YYYY-MM-DD HH:MM:SS based the time stamp of file creation
	 * @return file creation date time
	 */
	private String getFileCreationDateTime() throws IOException
	{
		Path                path            = Paths.get(getFilename());
		BasicFileAttributes fileAttr        = Files.readAttributes(path, BasicFileAttributes.class);
		FileTime            creationTime    = fileAttr.creationTime();
		String              cleanedDateTime = null; 
		
		if(creationTime.toString().matches(DATE_TIME_FORMAT))
		{
			cleanedDateTime = creationTime.toString();
			cleanedDateTime = cleanedDateTime.replace('T', ' ');
			cleanedDateTime = cleanedDateTime.substring(0, cleanedDateTime.length() - 2);
		}
		
		return cleanedDateTime;
	}
				
	private final String save_observation_script = "INSERT INTO Observation (source, variable_id, measurementunit_id)\n" +
			   									   "VALUES ('%s', %d, %d)\n" +
			   									   "RETURNING observation_id;";
	
	private final static String INSERT_OBSERVATION_HEADER = "wres.Observation(variableposition_id, " +
			  "observation_time, " +
			  "observed_value, " +
			  "measurementunit_id, " +
			  "source_id)";

	private final static String DATE_TIME_FORMAT = "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}.*Z";
	
	private String variable_name;
	private String datatype_code = "";
	private int time_interval = 0;
	private String time_series_identifier = "";
	private String series_description = "";
	private int first_month = 0;
	private int first_year = 0;
	private int last_month = 0;
	private int last_year = 0;
	private int values_per_record = 0;
	private String missing_data_symbol = "-999.00";
	private String accumulated_data_symbol = "-998.00";
	
	private String measuremnt_unit = "";
	private static int FIRST_OBS_VALUE_START_POS = 20;
		
	/**
	 * The current state of a script that will be sent to the database
	 */
	private StringBuilder currentScript = null;
	
	/**
	 * The current definition of the table in which the data will be saved
	 */
	private String currentTableDefinition = null;
	
	/**
	 * The number of values that will be inserted in the next sql call
	 */
	private int insertCount = 0;
	
	/**
	 * The delimiter between values for copy statements
	 */
	private static final String delimiter = "|";
	
	private static Float specifiedMinimumValue = Float.MIN_VALUE;
    private static Float specifiedMaximumValue = Float.MAX_VALUE;
    
    /**
	 * The value which indicates a null or invalid value from the source
	 */
	private Float currentMissingValue = null;
	
	/**
     * Alias for the system agnostic newline separator
     */
	private final static String NEWLINE = System.lineSeparator();
	
	/**
	 * The ID for the variable that is currently being parsed
	 */
	private Integer currentVariableID = null;
	
	/**
	 * The ID for the unit of measurement for the variable that is currently being parsed
	 */
	private Integer currentMeasurementUnitID = null;
	
	/**
	 * The name of the variable whose values are currently being parsed 
	 */
	private String currentVariableName = null;
	
	/**
	 * The ID for the current source file
	 */
	private Integer currentSourceID = null;
	
	private boolean detailsSpecified = false;
	
	private OffsetDateTime specifiedEarliestDate = null;
    private OffsetDateTime specifiedLatestDate = null;
    
   
	/**
	/* The date/time that the source was created
	*/
	private String creationDateTime = null;
	
	/**
	/* The number of values in datacard file
	*/
	private int entry_count = 0;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DatacardSource.class);
	
}
