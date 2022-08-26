package wres.io.reading.commaseparated;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.config.ConfigHelper;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.DataSource;
import wres.io.reading.ReadException;
import wres.io.reading.ReaderUtilities;
import wres.io.reading.Source;
import wres.io.reading.TimeSeriesTuple;
import wres.io.utilities.DataProvider;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.system.SystemSettings;
import wres.util.Strings;

public class CSVSource implements Source
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CSVSource.class );

    // It's probably worth making this configurable
    private static final String DELIMITER = ",";
    private static final String REFERENCE_DATETIME_COLUMN = "start_date";
    private static final String FEATURE_DESCRIPTION_COLUMN = "location_description";
    private static final String FEATURE_SRID_COLUMN = "location_srid";
    private static final String FEATURE_WKT_COLUMN = "location_wkt";
    private static final String TIMESCALE_IN_MINUTES_COLUMN = "timescale_in_minutes";
    private static final String TIMESCALE_FUNCTION_COLUMN = "timescale_function";

    private static final String DEFAULT_ENSEMBLE_NAME = "default";

    /** A placeholder reference datetime for timeseries without one. */
    //private static final Instant PLACEHOLDER_REFERENCE_DATETIME = Instant.MIN;

    private final Set<String> unconfiguredVariableNames = new HashSet<>( 1 );

    private final ThreadPoolExecutor ingestSaverExecutor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingIngestResults;
    private final List<IngestResult> ingested;
    private final TimeSeriesIngester timeSeriesIngester;
    private final DataSource dataSource;

    /**
     * Constructor.
     * @param timeSeriesIngester The time-series ingester
     * @param dataSource the data source information
     * @param systemSettings The system settings
     */
    public CSVSource( TimeSeriesIngester timeSeriesIngester,
                      DataSource dataSource,
                      SystemSettings systemSettings )
    {
        Objects.requireNonNull( timeSeriesIngester );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( dataSource );

        // See comments in wres.io.reading.WebSource for info on below approach.
        ThreadFactory csvIngest = new BasicThreadFactory.Builder()
                                                                  .namingPattern( "CSV Ingest %d" )
                                                                  .build();

        int concurrentCount = 3;
        BlockingQueue<Runnable> webClientQueue = new ArrayBlockingQueue<>( concurrentCount );
        this.ingestSaverExecutor = new ThreadPoolExecutor( concurrentCount,
                                                           concurrentCount,
                                                           systemSettings.poolObjectLifespan(),
                                                           TimeUnit.MILLISECONDS,
                                                           webClientQueue,
                                                           csvIngest );
        this.ingestSaverExecutor.setRejectedExecutionHandler( new ThreadPoolExecutor.AbortPolicy() );
        this.ingests = new ArrayBlockingQueue<>( concurrentCount );
        this.startGettingIngestResults = new CountDownLatch( concurrentCount );
        this.ingested = new ArrayList<>();
        this.timeSeriesIngester = timeSeriesIngester;
        this.dataSource = dataSource;
    }

    @Override
    public List<IngestResult> save()
    {
        try
        {
            return this.saveTimeSeries();
        }
        finally
        {
            this.shutdownNow();
        }
    }

    List<IngestResult> saveTimeSeries()
    {
        try
        {
            DataProvider data = DataProvider.fromCSV( this.getFileName(), DELIMITER );
            parseTimeSeries( data );
        }
        catch ( IOException e )
        {
            throw new ReadException( "Failed to read a CSV source.", e );
        }

        if ( !this.unconfiguredVariableNames.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LeftOrRightOrBaseline lrb = this.getDataSource()
                                            .getLeftOrRightOrBaseline();

            LOGGER.warn( "The following variable names were encountered in a {} forecast csv data source with URI {} "
                         + "that were not declared in the project: {}",
                         lrb,
                         this.getFileName(),
                         this.unconfiguredVariableNames );
        }

        return Collections.unmodifiableList( this.ingested );
    }

    /**
     * @return the file name
     */

    private URI getFileName()
    {
        return this.getDataSource()
                   .getUri();
    }

    private void parseTimeSeries( final DataProvider data ) throws IOException
    {
        TimeSeriesMetadata currentTimeSeriesMetadata;
        TimeSeriesMetadata lastTimeSeriesMetadata = null;
        String lastEnsembleName = null;
        SortedMap<String, SortedMap<Instant, Double>> ensembleValues = new TreeMap<>();

        while ( data.next() )
        {
            this.validateDataProvider( data );

            // Reference datetime is optional, many sources do not have any.
            Map<ReferenceTimeType, Instant> referenceTimes = new HashMap<>();

            if ( data.hasColumn( REFERENCE_DATETIME_COLUMN ) )
            {
                referenceTimes.put( ReferenceTimeType.UNKNOWN, data.getInstant( REFERENCE_DATETIME_COLUMN ) );
            }

            String variableName = data.getString( "variable_name" );
            String locationName = data.getString( "location" );
            String locationDescription = null;

            if ( data.hasColumn( FEATURE_DESCRIPTION_COLUMN ) )
            {
                locationDescription = data.getString( FEATURE_DESCRIPTION_COLUMN );
            }

            Integer locationSrid = null;

            if ( data.hasColumn( FEATURE_SRID_COLUMN ) )
            {
                locationSrid = data.getInt( FEATURE_SRID_COLUMN );
            }

            String locationWkt = null;

            if ( data.hasColumn( FEATURE_WKT_COLUMN ) )
            {
                locationWkt = data.getString( FEATURE_WKT_COLUMN );
            }

            String unitName = data.getString( "measurement_unit" );

            Integer timeScaleInMinutes = null;

            if ( data.hasColumn( TIMESCALE_IN_MINUTES_COLUMN ) )
            {
                timeScaleInMinutes = data.getInt( TIMESCALE_IN_MINUTES_COLUMN );
            }

            String timeScaleFunction = null;

            if ( data.hasColumn( TIMESCALE_FUNCTION_COLUMN ) )
            {
                timeScaleFunction = data.getString( TIMESCALE_FUNCTION_COLUMN );
            }

            TimeScaleOuter timeScale = null;

            if ( timeScaleInMinutes != null )
            {
                Duration duration = Duration.of( timeScaleInMinutes,
                                                 ChronoUnit.MINUTES );

                if ( timeScaleFunction != null )
                {
                    TimeScaleFunction function = TimeScaleFunction.valueOf( timeScaleFunction );
                    timeScale = TimeScaleOuter.of( duration, function );
                }
                else
                {
                    timeScale = TimeScaleOuter.of( duration );
                }
            }

            Geometry geometry = MessageFactory.getGeometry( locationName,
                                                            locationDescription,
                                                            locationSrid,
                                                            locationWkt );
            FeatureKey location = FeatureKey.of( geometry );
            currentTimeSeriesMetadata =
                    TimeSeriesMetadata.of( referenceTimes,
                                           timeScale,
                                           variableName,
                                           location,
                                           unitName );
            String ensembleName = this.getEnsembleName( data );
            Instant valueDate = data.getInstant( "value_date" );
            Double value = data.getDouble( "value" );

            if ( !currentTimeSeriesMetadata.equals( lastTimeSeriesMetadata ) )
            {
                // New timeseries needed, but first initiate ingest of previous.
                if ( Objects.nonNull( lastTimeSeriesMetadata ) )
                {
                    this.createAndIngestTimeSeries( lastTimeSeriesMetadata,
                                                    ensembleValues,
                                                    lastEnsembleName,
                                                    data.getRowIndex() + 1 );
                }

                // New timeseries.
                ensembleValues = new TreeMap<>();
            }
            else
            {
                LOGGER.debug( "Current {} equals previous {}",
                              currentTimeSeriesMetadata,
                              lastTimeSeriesMetadata );
            }

            if ( !ensembleName.equals( lastEnsembleName ) )
            {
                // Validate this ensemble trace hasn't already started
                if ( Objects.nonNull( ensembleValues.get( ensembleName ) ) )
                {
                    LOGGER.debug( "Ignoring issue with wonky data order, e.g. interleaving trace data..." );
                    /*
                    throw new PreIngestException( "While reading "
                                                  + this.getDataSource()
                                                        .getUri()
                                                  + " around line "
                                                  + data.getRowIndex()
                                                  + " could not create new "
                                                  + "ensemble trace "
                                                  + ensembleName
                                                  + " because it already "
                                                  + "was in the series." );
                     */
                }
                else
                {
                    // Add new ensemble trace.
                    ensembleValues.put( ensembleName, new TreeMap<>() );
                    LOGGER.debug( "Added ensembleName {}", ensembleName );
                }
            }

            // Get the currently-building trace
            SortedMap<Instant, Double> trace = ensembleValues.get( ensembleName );

            // If the trace is non-existent, create it and save it.
            if ( Objects.isNull( trace ) )
            {
                trace = new TreeMap<>();
                ensembleValues.put( ensembleName, trace );
            }

            // Save the data into ensembleValues using the trace stored there.
            trace.put( valueDate, value );

            lastTimeSeriesMetadata = currentTimeSeriesMetadata;
            lastEnsembleName = ensembleName;
        }

        if ( Objects.nonNull( lastTimeSeriesMetadata ) )
        {
            this.createAndIngestTimeSeries( lastTimeSeriesMetadata,
                                            ensembleValues,
                                            lastEnsembleName,
                                            data.getRowIndex() + 1 );
        }
        else if ( LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Did not find data to build a timeseries in {}, so data may not have been ingested from this "
                         + "source.",
                         this.getDataSource()
                             .getUri() );
        }

        this.completeIngest();
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
     * Build a timeseries out of temporary data structures and then ingests it.
     *
     * When there is a placeholder reference datetime, replace it with the
     * latest valid datetime found as "latest observation." This means there was
     * no reference datetime found in the CSV, but until the WRES db schema is
     * ready to store any kind of timeseries with 0, 1, or N reference datetimes
     * we are required to specify something here.
     *
     * @param lastTimeSeriesMetadata The metadata for most-recently-parsed data.
     * @param ensembleValues The most-recently-parsed data in sorted map form.
     * @param lastEnsembleName The most-recently-parsed ensemble name.
     * @param lineNumber The most-recently-parsed line number in the csv source.
     * @return A TimeSeries either of Double or Ensemble, ready for ingest.
     * @throws PreIngestException When something goes wrong.
     */
    private void createAndIngestTimeSeries( TimeSeriesMetadata timeSeriesMetadata,
                                            SortedMap<String, SortedMap<Instant, Double>> ensembleValues,
                                            String lastEnsembleName,
                                            int lineNumber )
    {
        LOGGER.debug( "Creating a time-series with {}, {}, {}, {}",
                      timeSeriesMetadata,
                      ensembleValues,
                      lastEnsembleName,
                      lineNumber );

        // Check if this is actually an ensemble or single trace
        if ( ensembleValues.size() == 1
             && ensembleValues.firstKey()
                              .equals( DEFAULT_ENSEMBLE_NAME ) )
        {
            TimeSeries<Double> timeSeries = ReaderUtilities.transform( timeSeriesMetadata,
                                                                       ensembleValues.get( DEFAULT_ENSEMBLE_NAME ),
                                                                       lineNumber,
                                                                       this.getDataSource()
                                                                           .getUri() );

            this.ingestSingleValuedTimeSeries( timeSeries );
        }
        else
        {
            TimeSeries<Ensemble> timeSeries = ReaderUtilities.transformEnsemble( timeSeriesMetadata,
                                                                                 ensembleValues,
                                                                                 lineNumber,
                                                                                 this.getDataSource()
                                                                                     .getUri() );

            this.ingestEnsembleTimeSeries( timeSeries );
        }
    }

    private String getEnsembleName( final DataProvider data )
    {
        String ensembleName = DEFAULT_ENSEMBLE_NAME;

        if ( data.hasColumn( "ensemble_name" ) )
        {
            ensembleName = data.getString( "ensemble_name" );
        }

        if ( data.hasColumn( "qualifier_id" ) )
        {
            ensembleName += ":" + data.getString( "qualifier_id" );
        }

        if ( data.hasColumn( "ensemblemember_id" ) )
        {
            ensembleName += ":" + data.getInt( "ensemblemember_id" );
        }

        return ensembleName;
    }


    private void validateDataProvider( final DataProvider dataProvider ) throws IngestException
    {
        String prefix = "Validation error(s) on line " +
                        ( dataProvider.getRowIndex() + 1 )
                        +
                        " in '"
                        +
                        this.getFileName()
                        +
                        "'"
                        +
                        System.lineSeparator();
        String suffix = System.lineSeparator() + "'" + this.getFileName() + "' cannot be ingested.";
        StringJoiner errorJoiner = new StringJoiner(
                                                     System.lineSeparator(),
                                                     prefix,
                                                     suffix );
        boolean valid = true;
        boolean hasColumn;

        hasColumn = dataProvider.hasColumn( REFERENCE_DATETIME_COLUMN );

        if ( hasColumn )
        {
            if ( !Strings.hasValue( dataProvider.getString(
                                                            REFERENCE_DATETIME_COLUMN ) ) )
            {
                errorJoiner.add( "The provided csv is missing valid '"
                                 + REFERENCE_DATETIME_COLUMN
                                 + "' data." );
                valid = false;
            }
            else
            {
                try
                {
                    dataProvider.getInstant( REFERENCE_DATETIME_COLUMN );
                }
                catch ( DateTimeParseException | ClassCastException e )
                {
                    errorJoiner.add(
                                     "The provided csv has invalid data within the '"
                                     + REFERENCE_DATETIME_COLUMN
                                     + "' column." );
                }
            }
        }

        hasColumn = dataProvider.hasColumn( "value_date" );

        if ( !hasColumn )
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'value_date' column." );
        }
        else if ( !Strings.hasValue( dataProvider.getString( "value_date" ) ) )
        {
            errorJoiner.add( "The provided csv is missing valid 'value_date' data." );
            valid = false;
        }
        else
        {
            try
            {
                dataProvider.getInstant( "value_date" );
            }
            catch ( DateTimeParseException | ClassCastException e )
            {
                errorJoiner.add( "The provided csv has invalid data within the 'value_date' column." );
            }
        }

        hasColumn = dataProvider.hasColumn( "variable_name" );


        if ( !hasColumn )
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'variable_name' column." );
        }
        else if ( !Strings.hasValue( dataProvider.getString( "variable_name" ) ) )
        {
            errorJoiner.add( "The provided csv is missing valid 'variable_name' data." );
            valid = false;
        }
        // Only validate if the variable name is declared: #95012
        else if ( Objects.isNull( this.getDataSource()
                                      .getContext()
                                      .getVariable() )
                  || !dataProvider.getString( "variable_name" )
                                  .equalsIgnoreCase( ConfigHelper.getVariableName( this.getDataSource()
                                                                                       .getContext() ) ) )
        {
            String foundVariable = dataProvider.getString( "variable_name" );
            this.unconfiguredVariableNames.add( foundVariable );
        }

        hasColumn = dataProvider.hasColumn( "location" );


        if ( !hasColumn )
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'location' column." );
        }
        else if ( !Strings.hasValue( dataProvider.getString( "location" ) ) )
        {
            errorJoiner.add( "The provided csv is missing valid 'location' data." );
            valid = false;
        }

        hasColumn = dataProvider.hasColumn( "measurement_unit" );

        if ( !hasColumn )
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'measurement_unit' column." );
        }
        else if ( !Strings.hasValue( dataProvider.getString( "measurement_unit" ) ) )
        {
            errorJoiner.add( "The provided csv is missing valid 'measurement_unit' data." );
            valid = false;
        }

        hasColumn = dataProvider.hasColumn( "value" );

        if ( !hasColumn )
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'value' column." );
        }
        else
        {
            try
            {
                dataProvider.getDouble( "value" );
            }
            catch ( ClassCastException e )
            {
                errorJoiner.add( "The provided csv has invalid data within the 'value' column." );
            }
        }

        if ( !valid )
        {
            throw new IngestException( errorJoiner.toString() );
        }
    }

    /**
     * Create an ingester for the given timeseries and begin ingest, add the
     * future to this.ingests as a side-effect.
     * @param timeSeries The timeSeries to ingest.
     * @throws IngestException When anything goes wrong related to ingest.
     */

    private void ingestEnsembleTimeSeries( wres.datamodel.time.TimeSeries<Ensemble> timeSeries )
            throws IngestException
    {
        TimeSeriesIngester timeSeriesIngester = this.getTimeSeriesIngester();

        Stream<TimeSeriesTuple> tupleStream = Stream.of( TimeSeriesTuple.ofEnsemble( timeSeries,
                                                                                     this.getDataSource() ) );
        Future<List<IngestResult>> futureIngestResult =
                this.ingestSaverExecutor.submit( () -> timeSeriesIngester.ingest( tupleStream,
                                                                                  this.getDataSource() ) );
        this.addIngestResults( futureIngestResult );
    }

    /**
     * Create an ingester for the given timeseries and begin ingest, add the
     * future to this.ingests as a side-effect.
     * @param timeSeries The timeSeries to ingest.
     * @throws IngestException When anything goes wrong related to ingest.
     */

    private void ingestSingleValuedTimeSeries( wres.datamodel.time.TimeSeries<Double> timeSeries )
            throws IngestException
    {
        TimeSeriesIngester timeSeriesIngester = this.getTimeSeriesIngester();

        Stream<TimeSeriesTuple> tupleStream = Stream.of( TimeSeriesTuple.ofSingleValued( timeSeries,
                                                                                         this.getDataSource() ) );
        Future<List<IngestResult>> futureIngestResult =
                this.ingestSaverExecutor.submit( () -> timeSeriesIngester.ingest( tupleStream,
                                                                                  this.getDataSource() ) );
        this.addIngestResults( futureIngestResult );
    }

    /**
     * Adds the ingest results.
     * @param futureIngestResult the ingest results
     */

    private void addIngestResults( Future<List<IngestResult>> futureIngestResult )
    {
        this.ingests.add( futureIngestResult );
        this.startGettingIngestResults.countDown();

        // See WebSource for comments on this approach.
        if ( this.startGettingIngestResults.getCount() <= 0 )
        {
            try
            {
                Future<List<IngestResult>> future =
                        this.ingests.take();
                List<IngestResult> ingestResults = future.get();
                this.ingested.addAll( ingestResults );
            }
            catch ( InterruptedException ie )
            {
                LOGGER.warn( "Interrupted while getting ingest results for CSV source "
                             + this.getDataSource() );
                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException ee )
            {
                String message = "While getting ingest results for CSV source "
                                 + this.getDataSource();
                throw new IngestException( message, ee );
            }
        }
    }

    private void completeIngest() throws IngestException
    {
        try
        {
            // Finish getting the remainder of ingest results.
            for ( Future<List<IngestResult>> future : this.ingests )
            {
                List<IngestResult> ingestResults = future.get();
                ingested.addAll( ingestResults );
            }
        }
        catch ( InterruptedException ie )
        {
            LOGGER.warn( "Interrupted while ingesting CSV data from "
                         + this.getDataSource(),
                         ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            throw new IngestException( "Failed to ingest NWM data from "
                                       + this.getDataSource(),
                                       ee );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Parsed and ingested {} timeseries from {}",
                          this.ingested.size(),
                          this.getDataSource()
                              .getUri() );
        }
    }


    /**
     * Shuts down executors.
     */
    private void shutdownNow()
    {
        List<Runnable> abandoned = this.ingestSaverExecutor.shutdownNow();

        if ( !abandoned.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LOGGER.warn( "Abandoned {} ingest tasks for CSV source {}",
                         abandoned.size(),
                         this.getDataSource() );
        }
    }
}
