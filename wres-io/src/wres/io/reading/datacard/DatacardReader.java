package wres.io.reading.datacard;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DataSourceConfig;
import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.config.ConfigHelper;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.TimeSeriesReader;
import wres.io.reading.TimeSeriesTuple;
import wres.io.reading.DataSource.DataDisposition;
import wres.statistics.generated.Geometry;
import wres.util.Strings;

/**
 * <p>A reader of observation-like, single-valued time-series from a source in Datacard format. This is a legacy format 
 * that is poorly documented, but still used by the U.S. National Weather Service. Further information can be found 
 * here:
 * 
 * <p><a href="https://www.weather.gov/media/owp/oh/hrl/docs/72datacard.pdf">Datacard</a> 
 * 
 * <p>The above link was last accessed: 20220801T18:00Z.
 * 
 * @author James Brown
 * @author Christopher Tubbs
 * @author Jesse Bickel
 */

public class DatacardReader implements TimeSeriesReader
{
    private static final Logger LOGGER = LoggerFactory.getLogger( DatacardReader.class );
    private static final Set<Double> IGNORABLE_VALUES = Set.of( -998.0, -999.0, -9999.0 );
    private static final int FIRST_OBS_VALUE_START_POS = 20;

    /**
     * @return an instance
     */

    public static DatacardReader of()
    {
        return new DatacardReader();
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        // Validate that the source contains a readable file
        ReaderUtilities.validateFileSource( dataSource, false );
        
        try
        {
            Path path = Paths.get( dataSource.getUri() );
            BufferedReader reader = Files.newBufferedReader( path, StandardCharsets.UTF_8 );
            return this.read( dataSource, reader );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a CSV source.", e );
        }
    }

    @Override
    public Stream<TimeSeriesTuple> read( DataSource dataSource, InputStream inputStream )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( inputStream );

        BufferedReader reader = new BufferedReader( new InputStreamReader( inputStream, StandardCharsets.UTF_8 ) );
        return this.read( dataSource, reader );
    }

    /**
     * Reads from a reader.
     * @param dataSource the data source
     * @param reader the reader
     * @return the stream of time-series
     */
    private Stream<TimeSeriesTuple> read( DataSource dataSource, BufferedReader reader )
    {
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( reader );

        // Validate the disposition of the data source
        ReaderUtilities.validateDataDisposition( dataSource, DataDisposition.DATACARD );
        
        // Get the lazy supplier of time-series data
        Supplier<TimeSeriesTuple> supplier = this.getTimeSeriesSupplier( dataSource, reader );

        // Generate a stream of time-series. Nothing is read here. Rather, as part of a terminal operation on this 
        // stream, each pull will read through to the supplier, then in turn to the data provider, and finally to 
        // the data source.
        return Stream.generate( supplier )
                     // Finite stream, proceeds while a time-series is returned
                     .takeWhile( Objects::nonNull )
                     // Close the data provider when the stream is closed
                     .onClose( () -> {
                         LOGGER.debug( "Detected a stream close event, closing an underlying stream reader." );
                         try
                         {
                             reader.close();
                         }
                         catch ( IOException e )
                         {
                             LOGGER.warn( "Failed to close a Datacard stream reader.", e );
                         }
                     } );
    }

    /**
     * Returns a supplier of time-series.
     * @param dataSource the data source
     * @param reader the buffered reader from which values should be acquired
     * @return the supplier
     */

    private Supplier<TimeSeriesTuple> getTimeSeriesSupplier( DataSource dataSource, BufferedReader reader )
    {
        AtomicBoolean returned = new AtomicBoolean();
        return () -> {
            try
            {
                // There is only one time-series per read, by design of this format/reader
                if ( !returned.getAndSet( true ) )
                {
                    TimeSeries<Double> timeSeries = this.readTimeSeriesFromSource( dataSource, reader );
                    return TimeSeriesTuple.ofSingleValued( timeSeries, dataSource );
                }

                // Null sentinel to stop iterating
                return null;
            }
            catch ( ProjectConfigException | IOException e )
            {
                throw new ReadException( "While reading a Datacard file from " + dataSource.getUri(), e );
            }
        };
    }

    /**
     * Returns a supplier of time-series.
     * @param dataSource the data source
     * @param reader the buffered reader from which values should be acquired
     * @return the supplier
     * @throws IOException if the source could not be read for any reason
     */

    private TimeSeries<Double> readTimeSeriesFromSource( DataSource dataSource, BufferedReader reader )
            throws IOException
    {
        AtomicInteger lineNumber = new AtomicInteger(); // Line number state, shared across methods
        DatacardMetadata basicMetadata = this.readMetadataFromSource( dataSource, reader, lineNumber );

        return this.readTimeSeriesValuesFromSource( dataSource,
                                                    reader,
                                                    basicMetadata,
                                                    lineNumber );
    }

    /**
     * Returns a supplier of time-series.
     * @param dataSource the data source
     * @param reader the buffered reader from which values should be acquired
     * @param lineNumber the line number to increment
     * @return the supplier
     * @throws IOException if the source could not be read for any reason
     */

    private DatacardMetadata readMetadataFromSource( DataSource dataSource,
                                                     BufferedReader reader,
                                                     AtomicInteger lineNumber )
            throws IOException
    {
        String variableName = null;
        String unit = null;
        String featureName = null;
        String featureDescription = null;
        int firstMonth;
        int firstYear;
        int valuesPerRecord;
        Duration timeStep;
        int obsValColWidth = 0;

        String line = this.getFirstNonCommentLine( reader, lineNumber );

        // Process the first non-comment line if found, which is one of two header lines.
        if ( line != null )
        {
            // Variable name
            variableName = line.substring( 14, 18 )
                               .strip();
            // Store measurement unit, which is to be processed later.
            unit = line.substring( 24, 28 )
                       .strip();

            // Process time interval
            String stripped = line.substring( 29, 31 )
                                  .strip();
            int hours = Integer.parseInt( stripped );
            timeStep = Duration.ofHours( hours );

            // #91908
            if ( line.length() >= 34 )
            {
                featureName = this.getFeatureName( line );
            }

            if ( line.length() > 50 )
            {
                featureDescription = this.getFeatureDescription( line );
            }
        }
        else
        {
            String message = "The Datacard file at "
                             + dataSource.getUri()
                             + " had unexpected syntax and could not be read.";

            throw new ReadException( message );
        }

        // Process the second non-comment header line, which includes some additional metadata
        int lastColIdx;
        if ( ( line = reader.readLine() ) != null )
        {
            lineNumber.incrementAndGet();
            String firstMonthString = line.substring( 0, 2 )
                                          .trim();
            firstMonth = Integer.parseInt( firstMonthString );
            String firstYearString = line.substring( 4, 8 )
                                         .trim();
            firstYear = Integer.parseInt( firstYearString );
            String valuesPerRecordString = line.substring( 19, 21 )
                                               .trim();
            valuesPerRecord = Integer.parseInt( valuesPerRecordString );
            lastColIdx = Math.min( 32, line.length() - 1 );

            if ( lastColIdx > 24 )
            {
                obsValColWidth = this.getValColWidth( line.substring( 24, lastColIdx ) );
            }
        }
        else
        {
            String message = "The Datacard file at "
                             + dataSource.getUri()
                             + " had unexpected syntax on line "
                             + ( lineNumber.get() + 1 )
                             + " and could not be read.";

            throw new ReadException( message );
        }

        Names names = new Names( variableName,
                                 unit,
                                 featureName,
                                 featureDescription );

        return new DatacardMetadata( names,
                                     firstMonth,
                                     firstYear,
                                     valuesPerRecord,
                                     timeStep,
                                     obsValColWidth );
    }

    /**
     * Gets the feature name.
     * 
     * @param line the line from which to acquire the feature name
     */
    private String getFeatureName( String line )
    {
        // Read up to character 45 or the EOL, whichever comes first: #91908
        int stop = 45;
        if ( line.length() < 45 )
        {
            stop = line.length();
        }

        // Location id. As of 5.0, use location name verbatim.
        return line.substring( 34, stop )
                   .strip();
    }

    /**
     * Gets the feature description.
     * 
     * @param line the line from which to acquire the feature description
     * @return the feature description
     */
    private String getFeatureDescription( String line )
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
            description = description.strip();
        }

        return description;
    }

    /**
     * Iterates the reader until the first non-comment line is discovered, updating the metadata with the line number.
     * 
     * @param reader the reader
     * @param lineNumber the line number to increment
     * @param basicMetadata the basic metadata, including the line number
     * @return the first non-comment line
     * @throws IOException if data could not be read for any reason
     */

    private String getFirstNonCommentLine( BufferedReader reader, AtomicInteger lineNumber ) throws IOException
    {
        // Skip comment lines.  It is assumed that comments only exist at the beginning of the file, which
        // I believe is consistent with format requirements.
        String line;
        while ( ( line = reader.readLine() ) != null && line.startsWith( "$" ) )
        {
            int number = lineNumber.incrementAndGet();
            LOGGER.debug( "Line {} was skipped because it was a comment line.", number );
        }

        LOGGER.debug( "The first non-comment line was: {}.", line );

        return line;
    }

    /**
     * Returns a supplier of time-series.
     * @param dataSource the data source
     * @param reader the buffered reader from which values should be acquired
     * @param basicMetadata the basic metadata associated with the time-series
     * @param lineNumber the line number to update
     * @return the supplier
     * @throws IOException if the source could not be read for any reason
     */

    private TimeSeries<Double> readTimeSeriesValuesFromSource( DataSource dataSource,
                                                               BufferedReader reader,
                                                               DatacardMetadata basicMetadata,
                                                               AtomicInteger lineNumber )
            throws IOException
    {
        SortedMap<Instant, Double> values = new TreeMap<>();

        // Onto the rest of the file...
        LocalDateTime localDateTime = LocalDateTime.of( basicMetadata.firstYear,
                                                        basicMetadata.firstMonth,
                                                        1,
                                                        0,
                                                        0,
                                                        0 );
        int valIdxInRecord;
        int startIdx;
        int endIdx;

        ZoneOffset offset = this.getZoneOffset( dataSource );

        Instant validDatetime = localDateTime.atOffset( offset )
                                             .toInstant();

        // Process the data lines one at a time.
        String line;
        while ( ( line = reader.readLine() ) != null )
        {
            lineNumber.incrementAndGet();
            line = Strings.rightTrim( line );

            // loop through all values in one line
            for ( valIdxInRecord = 0; valIdxInRecord < basicMetadata.valuesPerRecord; valIdxInRecord++ )
            {
                String value;
                startIdx = FIRST_OBS_VALUE_START_POS + valIdxInRecord * basicMetadata.obsValColWidth;

                // Have all values in the line been processed?
                if ( line.length() > startIdx )
                {
                    // Last value in the row/record?
                    if ( valIdxInRecord == basicMetadata.valuesPerRecord - 1 ||
                         ( FIRST_OBS_VALUE_START_POS
                           + ( valIdxInRecord + 1 ) * basicMetadata.obsValColWidth >= line.length() ) )
                    {
                        value = line.substring( startIdx );
                    }
                    else
                    {
                        endIdx = Math.min( startIdx + basicMetadata.obsValColWidth + 1, line.length() );
                        value = line.substring( startIdx, endIdx );
                    }

                    Double actualValue = this.getValueFromValueString( value,
                                                                       dataSource,
                                                                       valIdxInRecord,
                                                                       line,
                                                                       lineNumber.get() );

                    validDatetime = validDatetime.plus( basicMetadata.timeStep );

                    values.put( validDatetime, actualValue );
                }
                else
                {
                    // The last value of the line has been processed.
                    break;
                }
            }
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Parsed timeseries from '{}'", dataSource.getUri() );
        }

        Geometry geometry = MessageFactory.getGeometry( basicMetadata.featureName,
                                                        basicMetadata.featureDescription,
                                                        null,
                                                        null );
        FeatureKey location = FeatureKey.of( geometry );
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             // No time scale information: #92480 and #59536
                                                             null,
                                                             basicMetadata.variableName,
                                                             location,
                                                             basicMetadata.unit );
        return this.transform( metadata,
                               values,
                               lineNumber.get() );
    }

    /**
     * Acquires the zone offset from the data source, which is required.
     * @param dataSource the data source
     * @return the zone offset
     * @throws ProjectConfigException if the zone offset is not defined
     */

    private ZoneOffset getZoneOffset( DataSource dataSource )
    {
        DataSourceConfig.Source source = dataSource.getSource();

        // Zone offset is required configuration since datacard does not specify
        // its time zone.  Process it.
        ZoneOffset offset = ConfigHelper.getZoneOffset( source );
        LOGGER.debug( "The configured time zone offset is {}.", offset );

        if ( offset == null )
        {
            String message = "While reading Datacard source "
                             + dataSource.getUri()
                             + ", failed to discover a zoneOffset in the project declaration. Unfortunately, Datacard "
                             + "requires that the project declaration includes a zoneOffset such as "
                             + "zoneOffset=\"-0500\" or zoneOffset=\"EST\" or zoneOffset=\"Z\". Please add a correct "
                             + "zoneOffset for this Datacard file.";
            throw new ProjectConfigException( source, message );
        }

        return offset;
    }

    /**
     * Parses a string into a value, if possible.
     * @param value the value
     * @param dataSource the data source, to help with exception messaging
     * @param valIdxInRecord the value index, to help with exception messaging
     * @param line the data line, to help with exception messaging
     * @param lineNumber the current line number, to help with exception messaging
     * @return the value
     * @throws ReadException if the value could not be read into a real number
     */

    private Double getValueFromValueString( String value,
                                            DataSource dataSource,
                                            int valIdxInRecord,
                                            String line,
                                            int lineNumber )
    {
        Double actualValue;

        try
        {
            actualValue = Double.parseDouble( value );

            if ( this.valueIsIgnorable( actualValue )
                 || this.valueIsMissing( dataSource, actualValue ) )
            {
                actualValue = MissingValues.DOUBLE;
            }

            return actualValue;
        }
        catch ( NumberFormatException nfe )
        {
            String message = "While reading datacard file "
                             + dataSource.getUri()
                             + ", could not parse the value at position "
                             + valIdxInRecord
                             + " on this line ("
                             + lineNumber
                             + "): "
                             + line;

            throw new ReadException( message, nfe );
        }
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
            idxF = formatStr.toUpperCase()
                            .indexOf( 'F' );

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
        return DatacardReader.IGNORABLE_VALUES.contains( value );
    }

    private boolean valueIsMissing( DataSource dataSource, double value )
    {
        DataSourceConfig.Source source = dataSource.getSource();
        String missingValue = this.getMissingValue( source );

        return Objects.nonNull( missingValue )
               && Precision.equals( Double.parseDouble( missingValue ), value, Precision.EPSILON );
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

    /**
     * Hidden constructor.
     */
    private DatacardReader()
    {
    }

    /**
     * An immutable value class that stores metadata for sharing internally. TODO: candidate for a "Record" in JDK 17+.
     * 
     * @author James Brown
     */

    private static class DatacardMetadata
    {
        private final String variableName;
        private final String unit;
        private final String featureName;
        private final String featureDescription;
        private final int firstMonth;
        private final int firstYear;
        private final int valuesPerRecord;
        private final Duration timeStep;
        private final int obsValColWidth;

        private DatacardMetadata( Names names,
                                  int firstMonth,
                                  int firstYear,
                                  int valuesPerRecord,
                                  Duration timeStep,
                                  int obsValColWidth )
        {
            this.variableName = names.variableName;
            this.unit = names.unit;
            this.featureName = names.featureName;
            this.featureDescription = names.featureDescription;
            this.firstMonth = firstMonth;
            this.firstYear = firstYear;
            this.valuesPerRecord = valuesPerRecord;
            this.timeStep = timeStep;
            this.obsValColWidth = obsValColWidth;
        }
    }

    /**
     * An immutable value class to hold variable and feature names. TODO: candidate for a "Record" in JDK 17+.
     * @author James Brown
     */

    private static class Names
    {
        private final String variableName;
        private final String unit;
        private final String featureName;
        private final String featureDescription;

        private Names( String variableName, String unit, String featureName, String featureDescription )
        {
            this.variableName = variableName;
            this.unit = unit;
            this.featureName = featureName;
            this.featureDescription = featureDescription;
        }
    }

}
