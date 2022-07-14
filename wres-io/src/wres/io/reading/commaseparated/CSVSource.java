package wres.io.reading.commaseparated;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static wres.datamodel.time.ReferenceTimeType.LATEST_OBSERVATION;
import static wres.datamodel.time.ReferenceTimeType.UNKNOWN;

import wres.config.generated.LeftOrRightOrBaseline;
import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.TimeScales;
import wres.io.ingesting.IngestException;
import wres.io.ingesting.IngestResult;
import wres.io.ingesting.PreIngestException;
import wres.io.ingesting.TimeSeriesIngester;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.ReaderUtilities;
import wres.io.utilities.DataProvider;
import wres.io.utilities.Database;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.util.Strings;

public class CSVSource extends BasicSource
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
    private static final Instant PLACEHOLDER_REFERENCE_DATETIME = Instant.MIN;

    private final SystemSettings systemSettings;
    private final Database database;
    private final Features featuresCache;
    private final TimeScales timeScalesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final DatabaseLockManager lockManager;
    private final Set<String> unconfiguredVariableNames = new HashSet<>( 1 );

    private final ThreadPoolExecutor ingestSaverExecutor;
    private final BlockingQueue<Future<List<IngestResult>>> ingests;
    private final CountDownLatch startGettingIngestResults;
    private final List<IngestResult> ingested;


    /**
     * Constructor that sets the filename
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param featuresCache The features cache to use.
     * @param timeScalesCache The time scales cache to use.
     * @param ensemblesCache The ensembles cache to use.
     * @param measurementUnitsCache The measurement units cache to use.
     * @param projectConfig the ProjectConfig causing ingest
     * @param dataSource the data source information
     * @param lockManager The lock manager to use.
     */
    public CSVSource( SystemSettings systemSettings,
                      Database database,
                      Features featuresCache,
                      TimeScales timeScalesCache,
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
        this.timeScalesCache = timeScalesCache;
        this.ensemblesCache = ensemblesCache;
        this.measurementUnitsCache = measurementUnitsCache;
        this.lockManager = lockManager;

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

    private TimeScales getTimeScalesCache()
    {
        return this.timeScalesCache;
    }

    private Ensembles getEnsemblesCache()
    {
        return this.ensemblesCache;
    }

    private MeasurementUnits getMeasurementUnitsCache()
    {
        return this.measurementUnitsCache;
    }

    private DatabaseLockManager getLockManager()
    {
        return this.lockManager;
    }

    @Override
    protected List<IngestResult> saveObservation() throws IOException
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

    @Override
    protected List<IngestResult> saveForecast() throws IOException
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

    List<IngestResult> saveTimeSeries() throws IOException
    {
        DataProvider data = DataProvider.fromCSV( this.getFilename(), DELIMITER );
        parseTimeSeries( data );

        if ( !this.unconfiguredVariableNames.isEmpty() && LOGGER.isWarnEnabled() )
        {
            LeftOrRightOrBaseline lrb = ConfigHelper.getLeftOrRightOrBaseline( this.getProjectConfig(),
                                                                               this.getDataSourceConfig() );

            LOGGER.warn( "The following variable names were encountered in a {} forecast csv data source with URI {} "
                         + "that were not declared in the project: {}",
                         lrb,
                         this.getFilename(),
                         this.unconfiguredVariableNames );
        }

        return Collections.unmodifiableList( this.ingested );
    }

    private void parseTimeSeries(final DataProvider data) throws IOException
    {
        TimeSeriesMetadata currentTimeSeriesMetadata;
        TimeSeriesMetadata lastTimeSeriesMetadata = null;
        String lastEnsembleName = null;
        SortedMap<String,SortedMap<Instant,Double>> ensembleValues = new TreeMap<>();

        while (data.next())
        {
            this.validateDataProvider( data );

            // Reference datetime is optional, many sources do not have any.
            Instant referenceDatetime = null;

            if ( data.hasColumn( REFERENCE_DATETIME_COLUMN ) )
            {
                referenceDatetime = data.getInstant( REFERENCE_DATETIME_COLUMN );
            }
            else
            {
                // TimeSeriesMetadata (currently) requires a reference datetime,
                // so mark it with a placeholder. Later after getting all the
                // observations or simulation values, replace it with the latest
                // datetime on the valid-datetime-line.
                referenceDatetime = PLACEHOLDER_REFERENCE_DATETIME;
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
                    TimeSeriesMetadata.of( Map.of( UNKNOWN, referenceDatetime ),
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
                    TimeSeriesMetadata metadata = this.getTimeSeriesMetadata( lastTimeSeriesMetadata,
                                                                              ensembleValues,
                                                                              lastEnsembleName );
                    this.createAndIngestTimeSeries( metadata,
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
                             currentTimeSeriesMetadata, lastTimeSeriesMetadata );
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
            SortedMap<Instant,Double> trace = ensembleValues.get( ensembleName );

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
            // After reading all data, save the last timeseries.
            TimeSeriesMetadata metadata = this.getTimeSeriesMetadata( lastTimeSeriesMetadata, 
                                                                      ensembleValues, 
                                                                      lastEnsembleName );
            this.createAndIngestTimeSeries( metadata,
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
     * Creates the time-series metadata from the inputs.
     * @param lastTimeSeriesMetadata the last metadata
     * @param ensembleValues the ensemble values
     * @param lastEnsembleName the last ensemble name
     * @return the metadata
     */
    
    private TimeSeriesMetadata getTimeSeriesMetadata( TimeSeriesMetadata lastTimeSeriesMetadata,
                                                      SortedMap<String,SortedMap<Instant,Double>> ensembleValues,
                                                      String lastEnsembleName )
    {
        TimeSeriesMetadata metadata;
        Collection<Instant> referenceDatetimes =
                lastTimeSeriesMetadata.getReferenceTimes()
                                      .values();

        // When there are no reference datetimes, use latest value
        // by valid datetime. (Eventually we should remove the
        // restriction of requiring a reference datetime when db is
        // ready for it to be relaxed)
        if ( referenceDatetimes.size() == 1
             && referenceDatetimes.contains( PLACEHOLDER_REFERENCE_DATETIME ) )
        {
            LOGGER.debug( "Found placeholder reference datetime in {}",
                          lastTimeSeriesMetadata);
            Instant latestDatetime = ensembleValues.get( lastEnsembleName )
                                                   .lastKey();
            metadata = TimeSeriesMetadata.of( Map.of( LATEST_OBSERVATION, latestDatetime ),
                                              lastTimeSeriesMetadata.getTimeScale(),
                                              lastTimeSeriesMetadata.getVariableName(),
                                              lastTimeSeriesMetadata.getFeature(),
                                              lastTimeSeriesMetadata.getUnit() );
        }
        else
        {
            LOGGER.debug( "Found NO placeholder reference datetime in {}",
                          lastTimeSeriesMetadata);
            metadata = lastTimeSeriesMetadata;
        }
        
        return metadata;
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
                                                                       lineNumber );

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

        if (data.hasColumn("ensemble_name"))
        {
            ensembleName = data.getString("ensemble_name");
        }

        if (data.hasColumn("qualifier_id"))
        {
            ensembleName += ":" + data.getString("qualifier_id");
        }

        if (data.hasColumn("ensemblemember_id"))
        {
            ensembleName += ":" + data.getInt("ensemblemember_id");
        }

        return ensembleName;
    }


    private void validateDataProvider(final DataProvider dataProvider) throws IngestException
    {
        String prefix = "Validation error(s) on line " +
                        (dataProvider.getRowIndex() + 1) +
                        " in '" +
                        this.getFilename() +
                        "'" +
                        System.lineSeparator();
        String suffix = System.lineSeparator() + "'" + this.getFilename() + "' cannot be ingested.";
        StringJoiner errorJoiner = new StringJoiner(
                System.lineSeparator(),
                prefix,
                suffix
        );
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

        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'value_date' column." );
        }
        else if (!Strings.hasValue( dataProvider.getString( "value_date" ) ))
        {
            errorJoiner.add("The provided csv is missing valid 'value_date' data.");
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
                errorJoiner.add("The provided csv has invalid data within the 'value_date' column.");
            }
        }

        hasColumn = dataProvider.hasColumn( "variable_name" );


        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'variable_name' column." );
        }
        else if (!Strings.hasValue( dataProvider.getString( "variable_name" ) ))
        {
            errorJoiner.add("The provided csv is missing valid 'variable_name' data.");
            valid = false;
        }
        // Only validate if the variable name is declared: #95012
        else if ( Objects.isNull( this.getDataSourceConfig().getVariable() )
                  || !dataProvider.getString( "variable_name" )
                                  .equalsIgnoreCase( this.getDataSourceConfig().getVariable().getValue() ) )
        {
            String foundVariable = dataProvider.getString( "variable_name" );
            this.unconfiguredVariableNames.add( foundVariable );
        }

        hasColumn = dataProvider.hasColumn( "location" );


        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'location' column." );
        }
        else if (!Strings.hasValue( dataProvider.getString( "location" ) ))
        {
            errorJoiner.add("The provided csv is missing valid 'location' data.");
            valid = false;
        }

        hasColumn = dataProvider.hasColumn( "measurement_unit" );

        if (!hasColumn)
        {
            valid = false;
            errorJoiner.add( "The provided csv is missing a 'measurement_unit' column." );
        }
        else if (!Strings.hasValue( dataProvider.getString( "measurement_unit" ) ))
        {
            errorJoiner.add("The provided csv is missing valid 'measurement_unit' data.");
            valid = false;
        }

        hasColumn = dataProvider.hasColumn( "value" );

        if (!hasColumn)
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
                errorJoiner.add("The provided csv has invalid data within the 'value' column.");
            }
        }

        if (!valid)
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
        TimeSeriesIngester timeSeriesIngester =
                this.createTimeSeriesIngester( this.getSystemSettings(),
                                               this.getDatabase(),
                                               this.getFeaturesCache(),
                                               this.getTimeScalesCache(),
                                               this.getEnsemblesCache(),
                                               this.getMeasurementUnitsCache(),
                                               this.getProjectConfig(),
                                               this.getDataSource(),
                                               this.getLockManager() );

        Future<List<IngestResult>> futureIngestResult =
                this.ingestSaverExecutor.submit( () -> timeSeriesIngester.ingestEnsembleTimeSeries( timeSeries ) );
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
        TimeSeriesIngester timeSeriesIngester =
                this.createTimeSeriesIngester( this.getSystemSettings(),
                                               this.getDatabase(),
                                               this.getFeaturesCache(),
                                               this.getTimeScalesCache(),
                                               this.getEnsemblesCache(),
                                               this.getMeasurementUnitsCache(),
                                               this.getProjectConfig(),
                                               this.getDataSource(),
                                               this.getLockManager() );

        Future<List<IngestResult>> futureIngestResult =
                this.ingestSaverExecutor.submit( () -> timeSeriesIngester.ingestSingleValuedTimeSeries( timeSeries ) );
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
                         + this.getDataSource(), ie );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException ee )
        {
            throw new IngestException( "Failed to ingest NWM data from "
                                       + this.getDataSource(), ee );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Parsed and ingested {} timeseries from {}",
                          ingested.size(),
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
                         abandoned.size(), this.getDataSource() );
        }
    }

    @Override
    protected Logger getLogger()
    {
        return CSVSource.LOGGER;
    }


    /**
     * This method facilitates testing, Pattern 1 at
     * https://github.com/mockito/mockito/wiki/Mocking-Object-Creation
     * @return a TimeSeriesIngester
     */

    TimeSeriesIngester createTimeSeriesIngester( SystemSettings systemSettings,
                                                 Database database,
                                                 Features featuresCache,
                                                 TimeScales timeScalesCache,
                                                 Ensembles ensemblesCache,
                                                 MeasurementUnits measurementUnitsCache,
                                                 ProjectConfig projectConfig,
                                                 DataSource dataSource,
                                                 DatabaseLockManager lockManager )
    {
        return TimeSeriesIngester.of( systemSettings,
                                      database,
                                      featuresCache,
                                      timeScalesCache,
                                      ensemblesCache,
                                      measurementUnitsCache,
                                      projectConfig,
                                      dataSource,
                                      lockManager );
    }
}
