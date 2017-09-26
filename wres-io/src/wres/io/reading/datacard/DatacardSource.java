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
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;
import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.io.concurrency.CopyExecutor;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.utilities.Database;
import wres.util.ProgressMonitor;
import wres.util.Strings;
import wres.util.Time;

/**
 * @author ctubbs
 *
 */
@SuppressWarnings("deprecation")
public class DatacardSource extends BasicSource {

     
	/**
	 * 
	 * @param filename the file name
	 */
	public DatacardSource(String filename) {
		setFilename(filename);
		this.setHash();
	}
	
	public String getDatatypeCode()
	{
		return datatypeCode;
	}
	
	public void setDatatypeCode(String code)
	{
		datatypeCode = code.trim();
	}
	
	public int getTimeInterval()
	{
		return timeInterval;
	}
	
	public void setTimeInterval(int interval)
	{
		timeInterval = interval;
	}
	
	public void setTimeInterval(String interval)
	{
		setTimeInterval(Integer.parseInt(interval.trim()));
	}
	
	public String getSeriesDescription()
	{
		return seriesDescription;
	}
	
	public void setSeriesDescription(String description)
	{
		seriesDescription = description.trim();
	}
	
	public int getFirstMonth()
	{
		return firstMonth;
	}
	
	public void setFirstMonth(int month)
	{
		firstMonth = month;
	}
	
	public void setFirstMonth(String monthNumber)
	{
	    setFirstMonth(Integer.parseInt(monthNumber.trim()));
	}
	
	public int getFirstYear()
	{
		return firstYear;
	}
	
	public void setFirstYear(int year)
	{
		firstYear = year;
	}
	
	public void setFirstYear(String year)
	{
		year = year.trim();
		setFirstYear(Integer.parseInt(year));
	}
	
	public int getLastMonth()
	{
		return lastMonth;
	}
	
	public void setLastMonth(int month)
	{
		lastMonth = month;
	}
	
	public void setLastMonth(String monthNumber)
	{
	    setLastMonth(Integer.parseInt(monthNumber.trim()));
	}
	
	public int getLastYear()
	{
		return lastYear;
	}
	
	public void setLastYear(int year)
	{
		lastYear = year;
	}
	
	public void setLastYear(String year)
	{
	    setLastYear(Integer.parseInt(year.trim()));
	}
	
	public int getValuesPerRecord()
	{
		return valuesPerRecord;
	}
	
	public void setValuesPerRecord(String amount)
	{
		valuesPerRecord = Integer.parseInt(amount.trim());
	}
		
	public String getTimeSeriesIdentifier()
	{
		return timeSeriesIdentifier;
	}
	
	public void setTimeSeriesIdentifier(String identifier)
	{
		timeSeriesIdentifier = identifier.trim();
	}
		
	public String getMeasurementUnit()
	{
		return measurementUnit;
	}
	
	public void setMeasurementUnit(String unit)
	{
		measurementUnit = unit;
	}
	
	public int getMeasurementUnitId() throws SQLException 
	{
		int id = -1;
		
		id = MeasurementUnits.getMeasurementUnitID(measurementUnit);
				
		return id;
	}

	@Override
	public void saveObservation() throws IOException {
		Path path = Paths.get(getFilename());
		
		currentVariableName = getSpecifiedVariableName();
		measurementUnit     = getSpecifiedVariableUnit();
										
		try (BufferedReader reader = Files.newBufferedReader(path))
		{
			String line           = null;
			int    obsValColWidth = 0;
			int	   lastColIdx     = 0;
			
			//Process the comment lines. 
		    while ((line = reader.readLine()) != null && line.startsWith(getCommentLnSymbol()))
			{
				//skip comment lines
			}
			
			//First non-comment, header line.
			if (line != null)
			{
				setDatatypeCode(line.substring(14, 18));
				setTimeInterval(line.substring(29, 31));
				setTimeSeriesIdentifier(line.substring(34, 46));
				lastColIdx = Math.min(69, line.length() - 1);
				
				if(lastColIdx > 49)
				{
					setSeriesDescription(line.substring(49, lastColIdx));
				}
			}
			else
			{
				String message = "The NWS Datacard file ('%s') was not formatted correctly and could not be loaded correctly";
				throw new IOException(String.format(message, getFilename()));
			}
			
			//Second non-comment, header line.
			if ((line = reader.readLine()) != null)
			{
				setFirstMonth(line.substring(0, 2));
				setFirstYear(line.substring(4, 8));
				setLastMonth(line.substring(9, 11));
				setLastYear(line.substring(14, 18));
				setValuesPerRecord(line.substring(19, 21));
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
			
			OffsetDateTime datetime       = OffsetDateTime.of(getFirstYear(), getFirstMonth(), 1, 1, 0, 0, 0, ZoneOffset.UTC);
			String         timeZone       = getSpecifiedTimeZone();
			int            valIdxInRecord = 0;
			String         value          = "";
			
			int    startIdx   = 0;
			int    endIdx     = 0;
				
			datetime = datetime.minusHours(Time.zoneOffsetHours(timeZone));
            
			//Process the lines one at a time.
			while ((line = reader.readLine()) != null)
			{
				line = Strings.rtrim(line);
				
				// loop through all values in one line
				for (valIdxInRecord = 0; valIdxInRecord < valuesPerRecord; valIdxInRecord++)
				{
					value = "";
					startIdx = FIRST_OBS_VALUE_START_POS + valIdxInRecord * obsValColWidth;
					
					//Have all values in the line been processed?
					if (line.length() > startIdx)
					{
						//last value in the row/record?
						if(valIdxInRecord == valuesPerRecord - 1 || 
						  (FIRST_OBS_VALUE_START_POS + (valIdxInRecord + 1) * obsValColWidth >= line.length()))
						{
							value = line.substring(startIdx);
					    }
						else
						{
							endIdx = Math.min(startIdx + obsValColWidth + 1, line.length());
							value = line.substring(startIdx, endIdx);
						}
						
						datetime = datetime.plusHours(timeInterval);
						
						if (dateIsApproved(datetime.toString()) && valueIsApproved(value))
						{
							try
							{
								addObservedEvent(datetime.toString(), Float.parseFloat(value));
								entryCount++;
							}
							catch (Exception e) 
							{
								LOGGER.warn(value + " in datacard file not saved to database; cause: " + e.getMessage());
								throw new IOException("Unable to save datacard file data to database; cause: " + e.getMessage(), e);
		                    }
						}
					}
					else
					{
						//This line has less values. The last value of the line has been processed.
						break;
					}
				} //end of loop for one value line 
			} //end of loop for all value lines
			
			saveLeftoverObservations();
			
			try
			{
	        	this.getProjectDetails().addSource( this.getHash(), getDataSourceConfig() );
			}
	        catch(Exception e)
	        {
	        	throw new IOException ("Failed to add source for datacard " + e.getMessage());
	        }
		}
		finally
		{
			 if (LOGGER.isInfoEnabled())
	         {
				 LOGGER.info(String.valueOf(entryCount) + " values of datacardsource saved to database");
	         }
		}
	}
				
	private void saveLeftoverObservations()
    {
        if (insertCount > 0 && !testMode)
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
       	if (insertCount > 0)
		{
			currentScript.append(NEWLINE);
		}
		else
		{
			currentTableDefinition = INSERT_OBSERVATION_HEADER;
			currentScript = new StringBuilder();
		}
		
		currentScript.append(getVariablePositionID());
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
			
	public Integer getMeasurementID()  throws Exception
	{
		if(currentMeasurementUnitID == null)
		{
			currentMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(getSpecifiedVariableUnit());
		}
		
		return currentMeasurementUnitID ;
	}
	
	public void setMeasurementID(Integer id)  
	{
		currentMeasurementUnitID = id;
	}
	
	/**
	 * @return The ID of the variable currently being measured
	 * @throws Exception Thrown if interaction with the database failed.
	 */
	private int getVariableID() throws Exception
	{		
		if (currentVariableID == null)
		{
			this.currentVariableID = Variables.getVariableID(currentVariableName, measurementUnit);
		}
		
		return this.currentVariableID;
	}
	
	/**
	 * @return A valid ID for the source of this PIXML file from the database
	 * @throws Exception Thrown if an ID could not be retrieved from the database
	 */
	public int getSourceID() throws Exception
	{
		if (currentSourceID == null)
		{
			if (this.creationDateTime == null)
			{
				this.creationDateTime = getFileCreationDateTime();
			}
						
			currentSourceID = DataSources.getSourceID( getFilename(),
													   this.creationDateTime,
													   null,
													   Strings.getMD5Checksum(getFilename()));
		}
		
		return currentSourceID;
	}
	
	public void setSourceID(Integer id)  
	{
		currentSourceID = id;
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
	
    private boolean valueIsApproved(String value)
	{
    	String missingVal = getSpecifiedMissingValue();
		String accumVal   = getAccumlatedValue();
		Float  f          = null;
		boolean approved  = true;
		
		try
	    {
			f = Float.valueOf(value.trim());
			
			if(missingVal != null && accumVal != null)
			{
				approved = !isMissingValue(f, missingVal) && !f.equals(Float.valueOf(accumVal)) &&
						 f >= specifiedMinimumValue && f <= specifiedMaximumValue; 
			}
					   
	    }
	    catch (NumberFormatException nfe)
	    {
	    	approved = false;
	    	
	    	if (LOGGER.isInfoEnabled())
	    	{
	    		LOGGER.info(value + ": not approved value; Missing Val: " + missingVal + "; Accum Val: " + accumVal +
	    		" ; Range: [" + specifiedMinimumValue + ", " + specifiedMaximumValue + "]");
	    	}
	    }
    	
    	return approved;
	}
    
    private boolean isMissingValue(Float value, String specifiedMissingValues) throws NumberFormatException
    { 
    	boolean  isMissing = false;
    	String[] missingValArr = specifiedMissingValues.split(",");
    	
    	//loop through all comma delimited missing values specified in config file
    	for (String missingVal : missingValArr)
    	{
    		if(Float.valueOf(missingVal.trim()).equals(value))
    		{
    			isMissing = true;
    			break;
    		}
    	}
    	
    	return isMissing;
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
	   return entryCount;
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
			
			if(idxPeriod > idxF)
			{
				width = Integer.parseInt(formatStr.substring(idxF + 1, idxPeriod));
			}
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
			
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		long fileTimeInSeconds = creationTime.to(TimeUnit.SECONDS);
		LocalDateTime fileDateTime = LocalDateTime.ofEpochSecond( fileTimeInSeconds,
		                                                          0,
		                                                          ZoneOffset.UTC );
		
		String fileDateTimeStr = fileDateTime.format(formatter);
				
		if(!fileDateTimeStr.matches(DATE_TIME_FORMAT))
		{
			String errMsg = "Faied to parse file creation date/time" + creationTime.toString();
			
			LOGGER.error(errMsg);
			throw new IOException(errMsg);
		}
		
		return fileDateTimeStr;
	}
	
	/**
	 * Returns a accumulated value as a string
	 * @return accumulated value from config file 
	 */
	protected String getAccumlatedValue()
    {
        String accumalatedValue = null;

        if (dataSourceConfig != null)
        {
            DataSourceConfig.Source source = ConfigHelper.findDataSourceByFilename(dataSourceConfig, filename);

            if (source != null && source.getAccumulatedValue() != null && !source.getAccumulatedValue().isEmpty())
            {
            	accumalatedValue = source.getAccumulatedValue();
            }
        }

        return accumalatedValue;
    }
	
	/**
	 * Returns a symbol of comment line in DataCard file, from config file  as a string
	 * @return a symbol of comment line
	 */
	protected String getCommentLnSymbol()
    {
        String commentLnSymbol = "$";

        if (dataSourceConfig != null)
        {
            DataSourceConfig.Source source = ConfigHelper.findDataSourceByFilename(dataSourceConfig, this.filename);

            if (source != null && source.getCommentLnSymbol() != null && !source.getCommentLnSymbol().isEmpty())
            {
            	commentLnSymbol = source.getCommentLnSymbol();
            }
        }

        return commentLnSymbol;
    }
	
	/**
	 * Returns test mode as a boolean
	 * @return test mode 
	 */
	public boolean getTestMode()
	{
		return testMode;
	}
	
	/**
	 * Set test mode
	 * @param test Flag for test or not
	 */
	public void setTestMode(boolean test)
	{
		testMode = test;
	}
	
	public Integer getVariablePositionID() throws Exception
	{
		if(variablePositionID  == null)
		{
			variablePositionID = Features.getVariablePositionID(getSpecifiedLocationID(), getSpecifiedLocationID(), getVariableID());
		}
		
		return variablePositionID  ;
	}
	
	public void setVariablePositionID(Integer id)  
	{
		variablePositionID = id;
	}
			
	private final static String INSERT_OBSERVATION_HEADER = "wres.Observation(variableposition_id, " +
			  "observation_time, " +
			  "observed_value, " +
			  "measurementunit_id, " +
			  "source_id)";

	private final static String DATE_TIME_FORMAT = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";
	
	private String datatypeCode = "";
	private int timeInterval = 0;
	private String timeSeriesIdentifier = "";
	private String seriesDescription = "";
	private int firstMonth = 0;
	private int firstYear = 0;
	private int lastMonth = 0;
	private int lastYear = 0;
	private int valuesPerRecord = 0;
	
	private String measurementUnit = "";
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
	
	private Float specifiedMinimumValue = Float.MIN_VALUE;
    private Float specifiedMaximumValue = Float.MAX_VALUE;
    
	/**
     * Alias for the system agnostic newline separator
     */
	private static final String NEWLINE = System.lineSeparator();
	
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
	private int entryCount = 0;
	
	/**
	/* The flag for test or not
	*/
	private boolean testMode = false;
	
	/**
	/* The flag for test or not
	*/
	private Integer variablePositionID = null;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DatacardSource.class);
	
}

