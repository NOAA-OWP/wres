package wres.io.reading.datacard;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceDetails;
import wres.io.reading.BasicSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.InvalidInputDataException;
import wres.io.utilities.DataBuilder;
import wres.util.Strings;

/**
 * @author Qing Zhu
 *
 */
public class DatacardSource extends BasicSource
{
	private static final Double[] IGNORABLE_VALUES = {-998.0, -999.0, -9999.0};

    private String currentLocationId;

    private boolean inChargeOfIngest;

	/**
     * @param projectConfig the ProjectConfig causing ingest
	 * @param filename the file name
	 */
    public DatacardSource( ProjectConfig projectConfig,
                           URI filename)
    {
        super( projectConfig );
		setFilename(filename);
		this.setHash();
	}

    private void setTimeInterval(String interval)
    {
        this.timeInterval = Integer.parseInt(interval.trim());
    }

    private int getFirstMonth()
	{
		return firstMonth;
	}

    private void setFirstMonth(int month)
	{
		firstMonth = month;
	}

    private void setFirstMonth(String monthNumber)
	{
	    setFirstMonth(Integer.parseInt(monthNumber.trim()));
	}

    private int getFirstYear()
	{
		return firstYear;
	}

    private void setFirstYear(int year)
	{
		firstYear = year;
	}

    private void setFirstYear(String year)
	{
		year = year.trim();
		setFirstYear(Integer.parseInt(year));
	}

    private void setValuesPerRecord(String amount)
	{
		valuesPerRecord = Integer.parseInt(amount.trim());
	}

	@Override
    protected List<IngestResult> saveObservation() throws IOException
    {
        try
        {
            // This sets inChargeOfIngest (there may be a better way to do it).
            this.getSourceID();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "While retrieving source ID:", se );
        }

        if ( !this.inChargeOfIngest )
        {
            // Yield to another ingester task, say it was already found.
            return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                    this.getDataSourceConfig(),
                                                    this.getHash(),
                                                    this.getFilename(),
                                                    true );
        }

		Path path = Paths.get(getFilename());
										
		try (BufferedReader reader = Files.newBufferedReader(path))
		{
			String line;
			int lineNumber = 1;
			int obsValColWidth = 0;
			int lastColIdx;
			
			//Process the comment lines. 
		    while ((line = reader.readLine()) != null && line.startsWith("$"))
			{
				lineNumber++;
				LOGGER.debug( "Line {} was skipped because it was a comment line.",
                              lineNumber );
			}

            //First non-comment, header line.
            if (line != null)
            {
                setTimeInterval(line.substring(29, 31));
                if ( line.length() > 45 )
                {
                    String locationId = line.substring( 34, 45 );
                    // Currently using only the first five chars, trimmed
                    String lid = locationId.substring( 0, 5 )
                                           .trim();
                    setCurrentLocationId( lid );

                    if ( getSpecifiedLocationID() != null
                         && !getSpecifiedLocationID().isEmpty()
                         && !lid.isEmpty()
                         && !getSpecifiedLocationID().equalsIgnoreCase( lid ) )
                    {
                        String message = "Location identifier " + lid + " found"
                                         + " in the file "
                                         + this.getAbsoluteFilename()
                                         + " does not match what was specified"
                                         + " in the configuration "
                                         + getSpecifiedLocationID()
                                         + ". Please remove the attribute with "
                                         + getSpecifiedLocationID()
                                         + " from the config or change it to "
                                         + lid + ".";
                        throw new ProjectConfigException( getDataSourceConfig(),
                                                          message );
                    }
                }

                if ( getSpecifiedLocationID() != null
                     && !getSpecifiedLocationID().isEmpty() )
                {
                    setCurrentLocationId( getSpecifiedLocationID() );
                }
                else if ( getCurrentLocationId() == null
                          || getCurrentLocationId().isEmpty() )
                {
                    String message = "Could not find location ID in file "
                                     + this.getAbsoluteFilename()
									 + " nor was it specified in the project "
									 + "configuration. Please specify the lid "
									 + "in the source.";
                    throw new ProjectConfigException( getDataSourceConfig(),
													  message );
                }
            }
            else
            {
                String message = "The NWS Datacard file ('" + this.getFilename()
                                 + "') had unexpected syntax therefore it "
                                 + "could not be successfully read by WRES.";
                throw new InvalidInputDataException( message );
            }

			//Second non-comment, header line.
			if ((line = reader.readLine()) != null)
			{
			    lineNumber++;
				setFirstMonth(line.substring(0, 2));
				setFirstYear(line.substring(4, 8));
				setValuesPerRecord(line.substring(19, 21));
				lastColIdx = Math.min(32, line.length() - 1);
				
				if(lastColIdx > 24)
				{
					obsValColWidth = getValColWidth(line.substring(24, lastColIdx));
				}
			}
			else
            {
                String message = "The NWS Datacard file '" + this.getFilename()
                                 +"' had unexpected syntax on line "
                                 + lineNumber + 1 + " therefore it could not be"
                                 + " successfully read by WRES.";
                throw new InvalidInputDataException( message );
            }

            LocalDateTime  localDateTime  = LocalDateTime.of( getFirstYear(),
                                                              getFirstMonth(),
                                                              1,
                                                              0,
                                                              0,
                                                              0 );
            int valIdxInRecord;

			int startIdx;
			int endIdx;

            DataSourceConfig.Source source = this.getSourceConfig();

            ZoneOffset offset = ConfigHelper.getZoneOffset( source );
            LOGGER.debug( "{} is configured offset", offset );

            if ( offset == null )
            {
                String message = "While reading datacard source "
                                 + this.getFilename()
                                 + " WRES could not find a zoneOffset specified"
                                 + ". Datacard unfortunately requires that the "
                                 + "project configuration set a zoneOffset such"
                                 + " as zoneOffset=\"-0500\" or "
                                 + "zoneOffset=\"EST\" or zoneOffset=\"Z\". "
                                 + "Please discover and set the correct "
                                 + "zoneOffset for this data file.";
                throw new ProjectConfigException( source, message );
            }

            OffsetDateTime offsetDateTime = localDateTime.atOffset( offset );
            LocalDateTime utcDateTime =
                    offsetDateTime.withOffsetSameInstant( ZoneOffset.UTC )
                                  .toLocalDateTime();

			//Process the lines one at a time.
			while ((line = reader.readLine()) != null)
			{
			    lineNumber++;
				line = Strings.rightTrim( line);
				
				// loop through all values in one line
				for (valIdxInRecord = 0; valIdxInRecord < valuesPerRecord; valIdxInRecord++)
				{
                    String value;
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

                        Double actualValue;

                        try
                        {
                            actualValue = Double.parseDouble( value );

                            if (this.valueIsIgnorable( actualValue ) || this.valueIsMissing( actualValue ))
                            {
                                actualValue = null;
                            }
                        }
                        catch ( NumberFormatException nfe )
                        {
                            String message = "While reading datacard file "
                                             + this.getFilename()
                                             + ", could not parse the value at "
                                             + "position " + valIdxInRecord
                                             + " on this line (" + lineNumber + "): "
                                             + line;
                            throw new InvalidInputDataException( message, nfe );
                        }

                        utcDateTime = utcDateTime.plusHours( timeInterval );

                        IngestedValues.observed( actualValue )
                                      .at( utcDateTime )
                                      .measuredIn( this.getMeasurementID() )
                                      // Default is missing time scale: see #59536
                                      //.scaledBy( TimeScale.TimeScaleFunction.UNKNOWN )
                                      //.scaleOf( Duration.of(1, TimeHelper.LEAD_RESOLUTION) )
                                      .every( Duration.of( this.timeInterval, ChronoUnit.HOURS ) )
                                      .inSource( this.getSourceID() )
                                      .forVariableAndFeatureID( this.getVariableFeatureID() )
                                      .add();
                    }
                    else
					{
						//This line has less values. The last value of the line has been processed.
						break;
					}
				} //end of loop for one value line 
			} //end of loop for all value lines
		}
        catch ( SQLException e )
        {
            throw new IngestException( "Metadata used to save Datacard data could not be loaded.", e );
        }

		LOGGER.debug("Finished Parsing '{}'", this.getFilename());

        return IngestResult.singleItemListFrom( this.getProjectConfig(),
                                                this.getDataSourceConfig(),
                                                this.getHash(),
                                                this.getFilename(),
                                                false );
	}

    private Integer getMeasurementID() throws SQLException
	{
		if(currentMeasurementUnitID == null)
		{
			currentMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(getSpecifiedVariableUnit());
		}
		
		return currentMeasurementUnitID ;
	}

	/**
	 * @return The ID of the variable currently being measured
     * @throws SQLException when unable to retrieve an ID from the database
     */
    private int getVariableID() throws SQLException
    {
		if (currentVariableID == null)
		{
			this.currentVariableID = Variables.getVariableID(this.getSpecifiedVariableName());
		}
		
		return this.currentVariableID;
	}
	
	/**
	 * @return A valid ID for the source of this PIXML file from the database
     * @throws SQLException when an ID could not be retrieved from the database
     * @throws IOException when unable to get file attributes OR compute a hash
	 */
    private int getSourceID() throws IOException, SQLException
	{
		if (currentSourceID == null)
		{
			if (this.creationDateTime == null)
			{
				this.creationDateTime = getFileCreationDateTime();
			}

			// TODO: Modify the cache to do this work
            SourceDetails.SourceKey sourceKey =
                    new SourceDetails.SourceKey( this.getFilename(),
                                                 this.creationDateTime,
                                                 null,
                                                 this.getHash() );

            boolean wasInCache = DataSources.isCached( sourceKey );
            boolean wasThisReaderTheOneThatInserted = false;
            SourceDetails sourceDetails;


            if ( !wasInCache )
            {
                // We *might* be the one in charge of doing this source ingest.
                sourceDetails = new SourceDetails( sourceKey );
                sourceDetails.save();
                if ( sourceDetails.performedInsert() )
                {
                    // Now we have the definitive answer from the database.
                    wasThisReaderTheOneThatInserted = true;

                    // Now that ball is in our court we should put in cache
                    DataSources.put( sourceDetails );
                    // // Older, implicit way:
                    // DataSources.hasSource( this.getHash() );
                }
            }

            // Mark whether this reader is the one to perform ingest or yield.
            inChargeOfIngest = wasThisReaderTheOneThatInserted;

            // Regardless of whether we were the ones or not, get it from cache
            currentSourceID = DataSources.getActiveSourceID( this.getHash() );
		}

		return currentSourceID;
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
		int idxF;
		int idxPeriod;
		
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

    private Integer getVariableFeatureID() throws SQLException
	{
		if(VariableFeatureID  == null)
		{
            VariableFeatureID = Features.getVariableFeatureIDByLID( this.getCurrentLocationId(),
																	  getVariableID() );
		}
		
		return VariableFeatureID  ;
	}

    @Override
    protected String getValueToSave( final Double value )
    {
        String save;

        if (value == null || this.valueIsIgnorable( value ) || this.valueIsMissing( value ))
        {
            save = "\\N";
        }
        else
        {
            save = String.valueOf(value);
        }

        return save;
    }

	@Override
	protected Logger getLogger()
	{
		return DatacardSource.LOGGER;
	}

	private boolean valueIsIgnorable(final double value)
    {
        return wres.util.Collections.exists( DatacardSource.IGNORABLE_VALUES,
											 ignorable -> Precision.equals( value, ignorable, EPSILON ));
    }

    private boolean valueIsMissing(final double value)
    {
        return this.getSpecifiedMissingValue() != null &&
               Precision.equals( Double.parseDouble( this.getSpecifiedMissingValue()), value, EPSILON );
    }

    private String getCurrentLocationId()
    {
        return this.currentLocationId;
    }

    private void setCurrentLocationId( String currentLocationId )
    {
        this.currentLocationId = currentLocationId;
    }

	private static final String DATE_TIME_FORMAT = "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";

	private int firstMonth = 0;
	private int firstYear = 0;
	private int valuesPerRecord = 0;

	private static final int FIRST_OBS_VALUE_START_POS = 20;

    private final DataBuilder observations = DataBuilder.with(
            "variablefeature_id",
            "observation_time",
            "observed_value",
            "measurementunit_id",
            "source_id",
            "scale_period",
            "scale_function",
            "time_step"
    );

	/**
	 * The ID for the variable that is currently being parsed
	 */
	private Integer currentVariableID = null;
	
	/**
	 * The ID for the unit of measurement for the variable that is currently being parsed
	 */
	private Integer currentMeasurementUnitID = null;


    private int timeInterval = 0;
	/**
	 * The ID for the current source file
	 */
	private Integer currentSourceID = null;
   
	/**
	/* The date/time that the source was created
	*/
	private String creationDateTime = null;
	
	/**
	/* The number of values in datacard file
	*/
	private int entryCount = 0;

	private Integer VariableFeatureID = null;
	
	private static final Logger LOGGER = LoggerFactory.getLogger(DatacardSource.class);

}

