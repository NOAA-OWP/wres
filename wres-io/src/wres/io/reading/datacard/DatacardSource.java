package wres.io.reading.datacard;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.datamodel.time.ReferenceTimeType.LATEST_OBSERVATION;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.config.ConfigHelper;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.InvalidInputDataException;
import wres.io.reading.ReadException;
import wres.io.reading.Source;
import wres.statistics.generated.Geometry;
import wres.util.Strings;

public class DatacardSource implements Source
{
    private static final Set<Double> IGNORABLE_VALUES = Set.of( -998.0, -999.0, -9999.0 );

    private final TimeSeriesIngester timeSeriesIngester;
    private final DataSource dataSource;

    /**
     * @param timeSeriesIngester the time-series ingester
     * @param dataSource the data source information
     * @throws NullPointerException if any input is null
     */
    public DatacardSource( TimeSeriesIngester timeSeriesIngester,
                           DataSource dataSource )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( dataSource );
        
        this.timeSeriesIngester = timeSeriesIngester;
        this.dataSource = dataSource;
    }

    /**
     * @return the time-step
     */
    private Duration getTimeStep()
    {
        return this.timeStep;
    }

    /**
     * @return the time-series ingester
     */
    private TimeSeriesIngester getTimeSeriesIngester()
    {
        return this.timeSeriesIngester;
    }

    /**
     * @return the data source
     */
    private DataSource getDataSource()
    {
        return this.dataSource;
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

    private void setFirstMonth( int month )
    {
        firstMonth = month;
    }

    private void setFirstMonth( String monthNumber )
    {
        setFirstMonth( Integer.parseInt( monthNumber.trim() ) );
    }

    private int getFirstYear()
    {
        return firstYear;
    }

    private void setFirstYear( int year )
    {
        firstYear = year;
    }

    private void setFirstYear( String year )
    {
        year = year.trim();
        setFirstYear( Integer.parseInt( year ) );
    }

    private void setValuesPerRecord( String amount )
    {
        valuesPerRecord = Integer.parseInt( amount.trim() );
    }

    @Override
    public List<IngestResult> save()
    {
        Path path = Paths.get( this.getFileName() );
        String variableName = null;
        String unit = null;
        String featureName = null;
        String featureDescription = null;
        SortedMap<Instant, Double> values = new TreeMap<>();
        int lineNumber = 1;

        //Datacard reader.
        try ( BufferedReader reader = Files.newBufferedReader( path ) )
        {
            String line;
            int obsValColWidth = 0;
            int lastColIdx;

            //Skip comment lines.  It is assumed that comments only exist at the beginning of the file, which
            //I believe is consistent with format requirements.
            while ( ( line = reader.readLine() ) != null && line.startsWith( "$" ) )
            {
                lineNumber++;
                LOGGER.debug( "Line {} was skipped because it was a comment line.",
                              lineNumber );
            }

            //Process the first non-comment line if found, which is one of two header lines.
            if ( line != null )
            {
                // Variable name
                variableName = line.substring( 14, 18 )
                                   .strip();
                //Store measurement unit, which is to be processed later.
                unit = line.substring( 24, 28 )
                           .strip();

                //Process time interval.
                setTimeStep( line.substring( 29, 31 ) );
                // #91908
                if ( line.length() >= 34 )
                {
                    // Read up to character 45 or the EOL, whichever comes first: #91908
                    int stop = 45;
                    if ( line.length() < 45 )
                    {
                        stop = line.length();
                    }

                    // Location id. As of 5.0, use location name verbatim.
                    featureName = line.substring( 34, stop )
                                      .strip();
                }

                if ( line.length() > 50 )
                {
                    // Location description.
                    String description;

                    if ( line.length() <= 70 )
                    {
                        description = line.substring( 51 );
                    }
                    else
                    {
                        description = line.substring( 51, 70 );
                    }

                    if ( !description.isBlank() )
                    {
                        featureDescription = description.strip();
                    }
                }
            }
            else
            {
                String message = "The NWS Datacard file ('" + this.getFileName()
                                 + "') had unexpected syntax therefore it "
                                 + "could not be successfully read by WRES.";
                throw new InvalidInputDataException( message );
            }

            //Process the second non-comment header line.
            if ( ( line = reader.readLine() ) != null )
            {
                lineNumber++;
                setFirstMonth( line.substring( 0, 2 ) );
                setFirstYear( line.substring( 4, 8 ) );
                setValuesPerRecord( line.substring( 19, 21 ) );
                lastColIdx = Math.min( 32, line.length() - 1 );

                if ( lastColIdx > 24 )
                {
                    obsValColWidth = getValColWidth( line.substring( 24, lastColIdx ) );
                }
            }
            else
            {
                String message = "The NWS Datacard file '" + this.getFileName()
                                 + "' had unexpected syntax on line "
                                 + lineNumber
                                 + 1
                                 + " therefore it could not be"
                                 + " successfully read by WRES.";
                throw new InvalidInputDataException( message );
            }

            //Onto the rest of the file...
            LocalDateTime localDateTime = LocalDateTime.of( getFirstYear(),
                                                            getFirstMonth(),
                                                            1,
                                                            0,
                                                            0,
                                                            0 );
            int valIdxInRecord;
            int startIdx;
            int endIdx;

            DataSourceConfig.Source source = this.getDataSource()
                                                 .getSource();

            //Zone offset is required configuration since datacard does not specify
            //its time zone.  Process it.
            ZoneOffset offset = ConfigHelper.getZoneOffset( source );
            LOGGER.debug( "{} is configured offset", offset );

            if ( offset == null )
            {
                String message = "While reading datacard source "
                                 + this.getFileName()
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
            while ( ( line = reader.readLine() ) != null )
            {
                lineNumber++;
                line = Strings.rightTrim( line );

                // loop through all values in one line
                for ( valIdxInRecord = 0; valIdxInRecord < valuesPerRecord; valIdxInRecord++ )
                {
                    String value;
                    startIdx = FIRST_OBS_VALUE_START_POS + valIdxInRecord * obsValColWidth;

                    //Have all values in the line been processed?
                    if ( line.length() > startIdx )
                    {
                        //last value in the row/record?
                        if ( valIdxInRecord == valuesPerRecord - 1 ||
                             ( FIRST_OBS_VALUE_START_POS + ( valIdxInRecord + 1 ) * obsValColWidth >= line.length() ) )
                        {
                            value = line.substring( startIdx );
                        }
                        else
                        {
                            endIdx = Math.min( startIdx + obsValColWidth + 1, line.length() );
                            value = line.substring( startIdx, endIdx );
                        }

                        Double actualValue;

                        try
                        {
                            actualValue = Double.parseDouble( value );

                            if ( this.valueIsIgnorable( actualValue ) || this.valueIsMissing( actualValue ) )
                            {
                                actualValue = MissingValues.DOUBLE;
                            }
                        }
                        catch ( NumberFormatException nfe )
                        {
                            String message = "While reading datacard file "
                                             + this.getFileName()
                                             + ", could not parse the value at "
                                             + "position "
                                             + valIdxInRecord
                                             + " on this line ("
                                             + lineNumber
                                             + "): "
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
        catch( IOException e )
        {
            throw new ReadException( "Failed to read a Datacard source.", e );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Parsed timeseries from '{}'", this.getFileName() );
        }

        Geometry geometry = MessageFactory.getGeometry( featureName, featureDescription, null, null );
        FeatureKey location = FeatureKey.of( geometry );
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of(
                                                             Map.of( LATEST_OBSERVATION, values.lastKey() ),
                                                             // No time scale information: #92480 and #59536
                                                             null,
                                                             variableName,
                                                             location,
                                                             unit );
        TimeSeries<Double> timeSeries = this.transform( metadata,
                                                        values,
                                                        lineNumber );
        TimeSeriesIngester ingester = this.getTimeSeriesIngester();
        List<IngestResult> results = ingester.ingestSingleValuedTimeSeries( timeSeries, this.getDataSource() );

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Ingested {} timeseries from '{}'",
                          results.size(),
                          this.getFileName() );
        }

        return results;
    }

    /**
     * @return the file name
     */
    
    private URI getFileName()
    {
        return this.getDataSource()
                   .getUri();
    }
    
    /**
     * Return the number of columns of allocated for an observation value. In general, it is smaller than 
     * the number of columns actually used by an observation value
     * @param formatStr The float output format in FORTRAN 
     * @return Number of columns 
     */
    private int getValColWidth( String formatStr )
    {
        int width = 0;
        int idxF;
        int idxPeriod;

        if ( formatStr != null && formatStr.length() > 3 )
        {
            idxF = formatStr.toUpperCase().indexOf( 'F' );
            idxPeriod = formatStr.indexOf( '.' );

            if ( idxPeriod > idxF )
            {
                width = Integer.parseInt( formatStr.substring( idxF + 1, idxPeriod ) );
            }
        }

        return width;
    }

    private boolean valueIsIgnorable( final double value )
    {
        return DatacardSource.IGNORABLE_VALUES.contains( value );
    }

    private boolean valueIsMissing( final double value )
    {
        DataSourceConfig.Source source = this.getDataSource()
                                             .getSource();

        String missingValue = this.getMissingValue( source );

        return missingValue != null && Precision.equals( Double.parseDouble( missingValue ), value );
    }


    /**
     * @return The value specifying a value that is missing from the data set
     * originating from the data source configuration. While parsing the data,
     * if this value is encountered, it indicates that the value should be
     * ignored as it represents invalid data. This should be ignored in data
     * sources that define their own missing value.
     */
    private String getMissingValue( DataSourceConfig.Source source )
    {
        Objects.requireNonNull( source );

        String missingValue = null;

        if ( source.getMissingValue() != null && !source.getMissingValue().isEmpty() )
        {
            missingValue = source.getMissingValue();

            if ( missingValue.lastIndexOf( '.' ) + 6 < missingValue.length() )
            {
                missingValue = missingValue.substring( 0, missingValue.lastIndexOf( '.' ) + 6 );
            }
        }

        return missingValue;
    }

    private int firstMonth = 0;
    private int firstYear = 0;
    private int valuesPerRecord = 0;

    private static final int FIRST_OBS_VALUE_START_POS = 20;
    private Duration timeStep = Duration.ZERO;


    private static final Logger LOGGER = LoggerFactory.getLogger( DatacardSource.class );


    /**
     * Transform a single trace into a TimeSeries of doubles.
     * @param metadata The metadata of the timeseries.
     * @param trace The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @return The complete TimeSeries
     */

    private TimeSeries<Double> transform( TimeSeriesMetadata metadata,
                                          SortedMap<Instant, Double> trace,
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

        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, Double> events : trace.entrySet() )
        {
            Event<Double> event = Event.of( events.getKey(), events.getValue() );
            builder.addEvent( event );
        }

        return builder.build();
    }
}
