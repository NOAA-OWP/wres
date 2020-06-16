package wres.io.reading.datacard;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.datamodel.time.ReferenceTimeType.LATEST_OBSERVATION;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MissingValues;
import wres.datamodel.FeatureKey;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.reading.InvalidInputDataException;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.util.Strings;

public class DatacardSource extends BasicSource
{
    private static final Set<Double> IGNORABLE_VALUES = Set.of( -998.0, -999.0, -9999.0 );

	private final SystemSettings systemSettings;
	private final Database database;
	private final Features featuresCache;
	private final Variables variablesCache;
    private final Ensembles ensemblesCache;
	private final MeasurementUnits measurementUnitsCache;
	private final DatabaseLockManager lockManager;


	/**
	 * @param systemSettings The system settings to use.
	 * @param database The database to use.
	 * @param featuresCache The features cache to use.
	 * @param variablesCache The variables cache to use.
	 * @param ensemblesCache The ensembles cache to use.
	 * @param measurementUnitsCache The measurement units cache to use.
     * @param projectConfig the ProjectConfig causing ingest
     * @param dataSource the data source information
	 * @param lockManager The lock manager to use.
	 */
    public DatacardSource( SystemSettings systemSettings,
						   Database database,
						   Features featuresCache,
						   Variables variablesCache,
                           Ensembles ensemblesCache,
						   MeasurementUnits measurementUnitsCache,
						   ProjectConfig projectConfig,
                           DataSource dataSource,
                           DatabaseLockManager lockManager )
    {
        super( projectConfig, dataSource );
        this.systemSettings = systemSettings;
		this.database = database;
		this.featuresCache = featuresCache;
        this.ensemblesCache = ensemblesCache;
		this.variablesCache = variablesCache;
		this.measurementUnitsCache = measurementUnitsCache;
        this.lockManager = lockManager;
	}

	private SystemSettings getSystemSettings()
	{
		return this.systemSettings;
	}

	private Database getDatabase()
	{
		return this.database;
	}

	private Features getFeaturesCache()
	{
		return this.featuresCache;
	}

	private Variables getVariablesCache()
	{
		return this.variablesCache;
	}

	private MeasurementUnits getMeasurementUnitsCache()
	{
		return this.measurementUnitsCache;
	}

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    private Duration getTimeStep()
    {
        return this.timeStep;
    }

    /**
     * @param interval A string containing integer hours.
     */
    private void setTimeStep( String interval )
    {
        String stripped = interval.strip();
        int hours = Integer.parseInt( stripped );
        this.timeStep = Duration.ofHours( hours );
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
		Path path = Paths.get(getFilename());
        String variableName = null;
        String unit = null;
        String featureName = null;
        SortedMap<Instant,Double> values = new TreeMap<>();
        int lineNumber = 1;

		//Datacard reader.
		try (BufferedReader reader = Files.newBufferedReader(path))
		{
			String line;
			int obsValColWidth = 0;
			int lastColIdx;

			//Skip comment lines.  It is assumed that comments only exist at the beginning of the file, which
			//I believe is consistent with format requirements.
		    while ((line = reader.readLine()) != null && line.startsWith("$"))
			{
				lineNumber++;
				LOGGER.debug( "Line {} was skipped because it was a comment line.",
                              lineNumber );
			}

            //Process the first non-comment line if found, which is one of two header lines.
            if (line != null)
            {
                // Variable name
                variableName = line.substring( 14, 18 )
                                   .strip();
                //Store measurement unit, which is to be processed later.
                unit = line.substring( 24, 28 )
                           .strip();

                //Process time interval.
                setTimeStep( line.substring( 29, 31) );
                if ( line.length() > 45 )
                {
                    //Location id.  Currently using only the first five chars, trimmed.  Trimming is dangerous.
                    String locationId = line.substring( 34, 45 )
                                            .strip();
                    featureName = locationId.substring( 0, 5 )
                                            .strip();

                    if ( !locationId.equalsIgnoreCase( featureName ) )
                    {
                        LOGGER.warn( "Treating location name '{}' as '{}'.",
                                     locationId, featureName );
                    }

                    //Check for conflict between found location id and ids specified in configuration.
                    if ( getSpecifiedLocationID() != null
                         && !getSpecifiedLocationID().isEmpty()
                         && !featureName.isEmpty()
                         && !getSpecifiedLocationID().equalsIgnoreCase( featureName ) )
                    {
                        String message = "Location name '" + featureName
                                         + "' found in "
                                         + this.getFilename()
                                         + " does not match what was specified"
                                         + " in the configuration: "
                                         + getSpecifiedLocationID()
                                         + ". Please remove the attribute with "
                                         + getSpecifiedLocationID()
                                         + " from the config or change it to '"
                                         + featureName + "'.";
                        throw new ProjectConfigException( getDataSourceConfig(),
                                                          message );
                    }
                }

                if ( getSpecifiedLocationID() != null
                     && !getSpecifiedLocationID().isEmpty() )
                {
                    featureName = getSpecifiedLocationID();
                }
                else if ( featureName == null
                          || featureName.isEmpty() )
                {
                    String message = "Could not find feature name in "
                                     + this.getFilename()
                                     + " nor was it specified in the project "
                                     + "configuration. Please specify the "
                                     + "feature name in the source config.";
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

			//Process the second non-comment header line.
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

			//Onto the rest of the file...
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

            //Zone offset is required configuration since datacard does not specify
            //its time zone.  Process it.
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

            Instant validDatetime = localDateTime.atOffset( offset )
                                                 .toInstant();

			//Process the data lines one at a time.
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
                                actualValue = MissingValues.DOUBLE;
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

                        validDatetime = validDatetime.plus( this.getTimeStep() );

                        values.put( validDatetime, actualValue );
                    }
                    else
					{
						//This line has less values. The last value of the line has been processed.
						break;
					}
				} //end of loop for one value line 
			} //end of loop for all value lines
		}

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Parsed timeseries from '{}'", this.getFilename() );
        }

        FeatureKey location = new FeatureKey( featureName, null, null, null );
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of(
                Map.of( LATEST_OBSERVATION, values.lastKey() ),
                TimeScale.of(),
                variableName,
                location,
                unit );
        TimeSeries<Double> timeSeries = transform( metadata,
                                                   values,
                                                   lineNumber );
        TimeSeriesIngester ingester =
                TimeSeriesIngester.of( this.getSystemSettings(),
                                       this.getDatabase(),
                                       this.getFeaturesCache(),
                                       this.getVariablesCache(),
                                       this.getEnsemblesCache(),
                                       this.getMeasurementUnitsCache(),
                                       this.getProjectConfig(),
                                       this.getDataSource(),
                                       this.getLockManager(),
                                       timeSeries );
        List<IngestResult> results = ingester.call();

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Ingested {} timeseries from '{}'",
                         results.size(), this.getFilename() );
        }

        return results;
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

	@Override
	protected Logger getLogger()
	{
		return DatacardSource.LOGGER;
	}

	private boolean valueIsIgnorable(final double value)
    {
        return DatacardSource.IGNORABLE_VALUES.contains( value );
    }

    private boolean valueIsMissing(final double value)
    {
        return this.getSpecifiedMissingValue() != null &&
               Precision.equals( Double.parseDouble( this.getSpecifiedMissingValue()), value );
    }


	private int firstMonth = 0;
	private int firstYear = 0;
	private int valuesPerRecord = 0;

	private static final int FIRST_OBS_VALUE_START_POS = 20;
    private Duration timeStep = Duration.ZERO;

	
	private static final Logger LOGGER = LoggerFactory.getLogger(DatacardSource.class);


    /**
     * Transform a single trace into a TimeSeries of doubles.
     * @param metadata The metadata of the timeseries.
     * @param trace The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @return The complete TimeSeries
     */

    private TimeSeries<Double> transform( TimeSeriesMetadata metadata,
                                          SortedMap<Instant,Double> trace,
                                          int lineNumber )
    {
        if ( trace.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot transform fewer than "
                                                + "one values into timeseries "
                                                + "with metadata "
                                                + metadata
                                                + " from line number "
                                                + lineNumber );
        }

        TimeSeries.TimeSeriesBuilder<Double> builder = new TimeSeries.TimeSeriesBuilder<>();
        builder.setMetadata( metadata );

        for ( Map.Entry<Instant,Double> events : trace.entrySet() )
        {
            Event<Double> event = Event.of( events.getKey(), events.getValue() );
            builder.addEvent( event );
        }

        return builder.build();
    }
}
