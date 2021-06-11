package wres.io.reading.commaseparated;

import java.io.IOException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.TreeSet;
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

import wres.config.generated.ProjectConfig;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.concurrency.TimeSeriesIngester;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.PreIngestException;
import wres.io.utilities.DataProvider;
import wres.io.utilities.Database;
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

    private static final String DEFAULT_ENSEMBLE_NAME = "default";

    /** A placeholder reference datetime for timeseries without one. */
    private static final Instant PLACEHOLDER_REFERENCE_DATETIME = Instant.MIN;

    private final SystemSettings systemSettings;
    private final Database database;
    private final Features featuresCache;
    private final Variables variablesCache;
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
     * @param variablesCache The variables cache to use.
     * @param ensemblesCache The ensembles cache to use.
     * @param measurementUnitsCache The measurement units cache to use.
     * @param projectConfig the ProjectConfig causing ingest
     * @param dataSource the data source information
     * @param lockManager The lock manager to use.
     */
    public CSVSource( SystemSettings systemSettings,
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
        this.variablesCache = variablesCache;
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

    private Variables getVariablesCache()
    {
        return this.variablesCache;
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
        DataProvider data =  DataProvider.fromCSV( this.getFilename(), DELIMITER );
        parseTimeSeries( data );

        if ( !this.unconfiguredVariableNames.isEmpty() )
        {
            LOGGER.warn( "The following variable names were encountered in forecast csv data source from {} that were not configured in the project: {}",
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
                locationDescription = data.getString( "location_description" );
            }

            Integer locationSrid = null;

            if ( data.hasColumn( FEATURE_SRID_COLUMN ) )
            {
                locationSrid = data.getInt( "location_srid" );
            }

            String locationWkt = null;

            if ( data.hasColumn( FEATURE_WKT_COLUMN ) )
            {
                locationWkt = data.getString( "location_wkt" );
            }

            String unitName = data.getString( "measurement_unit" );
            FeatureKey location = new FeatureKey( locationName,
                                                  locationDescription,
                                                  locationSrid,
                                                  locationWkt );
            currentTimeSeriesMetadata =
                    TimeSeriesMetadata.of( Map.of( UNKNOWN, referenceDatetime ),
                                           null,
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
                    TimeSeries<?> timeSeries = this.buildTimeSeries( lastTimeSeriesMetadata,
                                                                     ensembleValues,
                                                                     lastEnsembleName,
                                                                     data.getRowIndex() + 1 );
                    this.ingest( timeSeries );
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

        if ( Objects.nonNull( lastTimeSeriesMetadata) )
        {
            // After reading all data, save the last timeseries.
            TimeSeries<?> timeSeries =
                    this.buildTimeSeries( lastTimeSeriesMetadata,
                                          ensembleValues,
                                          lastEnsembleName,
                                          data.getRowIndex() + 1 );
            this.ingest( timeSeries );
        }
        else if ( LOGGER.isWarnEnabled() )
        {
                LOGGER.warn( "Did not find data to build a timeseries in {}, so data may not have been ingested from this source.",
                             this.getDataSource()
                                 .getUri() );
        }

        this.completeIngest();
    }


    /**
     * Build a timeseries out of temporary data structures.
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
    private TimeSeries<?> buildTimeSeries( TimeSeriesMetadata lastTimeSeriesMetadata,
                                           SortedMap<String,SortedMap<Instant,Double>> ensembleValues,
                                           String lastEnsembleName,
                                           int lineNumber )
    {
        LOGGER.debug( "buildTimeSeries called with {}, {}, {}, {}",
                      lastTimeSeriesMetadata,
                      ensembleValues,
                      lastEnsembleName,
                      lineNumber );
        TimeSeries<?> timeSeries;
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

        // Check if this is actually an ensemble or single trace
        if ( ensembleValues.size() == 1
             && ensembleValues.firstKey()
                              .equals( DEFAULT_ENSEMBLE_NAME ) )
        {
            timeSeries = this.transform( metadata,
                                         ensembleValues.get( DEFAULT_ENSEMBLE_NAME ),
                                         lineNumber );
        }
        else
        {
            timeSeries = this.transformEnsemble( metadata,
                                                 ensembleValues,
                                                 lineNumber );
        }

        LOGGER.debug( "transformed into {}", timeSeries );
        return timeSeries;
    }

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

        TimeSeries.Builder<Double> builder = new TimeSeries.Builder<>();
        builder.setMetadata( metadata );

        for ( Map.Entry<Instant,Double> events : trace.entrySet() )
        {
            Event<Double> event = Event.of( events.getKey(), events.getValue() );
            builder.addEvent( event );
        }

        return builder.build();
    }

    /**
     * Transform a map of traces into a TimeSeries of ensembles (flip it) but
     * also validate the density and valid datetimes of the ensemble prior.
     * @param metadata The metadata of the timeseries.
     * @param traces The raw data to build a TimeSeries.
     * @param lineNumber The approximate location in the source.
     * @return The complete TimeSeries
     * @throws IllegalArgumentException When fewer than two traces given.
     * @throws PreIngestException When ragged (non-dense) data given.
     */

    private TimeSeries<Ensemble> transformEnsemble( TimeSeriesMetadata metadata,
                                                    SortedMap<String,SortedMap<Instant,Double>> traces,
                                                    int lineNumber )
    {
        int traceCount = traces.size();

        if ( traceCount < 2 )
        {
            throw new IllegalArgumentException( "Cannot transform fewer than "
                                                + "two traces into ensemble: "
                                                + traces );
        }

        Map<Instant,double[]> reshapedValues = null;
        Map.Entry<String,SortedMap<Instant,Double>> previousTrace = null;
        int i = 0;

        for ( Map.Entry<String,SortedMap<Instant,Double>> trace : traces.entrySet() )
        {
            SortedSet<Instant> theseInstants = new TreeSet<>( trace.getValue()
                                                                   .keySet() );

            if ( Objects.nonNull( previousTrace ) )
            {
                SortedSet<Instant> previousInstants = new TreeSet<>( previousTrace.getValue()
                                                                                  .keySet() );
                if ( !theseInstants.equals( previousInstants ) )
                {
                    throw new PreIngestException( "Cannot build ensemble from "
                                                  + this.getDataSource()
                                                        .getUri()
                                                  + " with data at or before "
                                                  + "line number "
                                                  + lineNumber
                                                  + " because the trace named "
                                                  + trace.getKey()
                                                  + " had these valid datetimes"
                                                  + ": " + theseInstants
                                                  + " but previous trace named "
                                                  + previousTrace.getKey()
                                                  + " had different ones: "
                                                  + previousInstants
                                                  + " which is not allowed. All"
                                                  + " traces must be dense and "
                                                  + "match valid datetimes." );
                }
            }

            if ( Objects.isNull( reshapedValues ) )
            {
                reshapedValues = new HashMap<>( theseInstants.size() );
            }

            for ( Map.Entry<Instant,Double> event : trace.getValue()
                                                         .entrySet() )
            {
                Instant validDateTime = event.getKey();

                if ( !reshapedValues.containsKey( validDateTime ) )
                {
                    reshapedValues.put( validDateTime, new double[traceCount] );
                }

                double[] values = reshapedValues.get( validDateTime );
                values[i] = event.getValue();
            }

            previousTrace = trace;
            i++;
        }

        wres.datamodel.time.TimeSeries.Builder<Ensemble> builder =
                new wres.datamodel.time.TimeSeries.Builder<>();

        // Because the iteration is over a sorted map, assuming same order here.
        SortedSet<String> traceNamesSorted = new TreeSet<>( traces.keySet() );
        String[] traceNames = new String[traceNamesSorted.size()];
        traceNamesSorted.toArray( traceNames );
        Labels labels = Labels.of( traceNames );

        builder.setMetadata( metadata );

        for ( Map.Entry<Instant, double[]> events : reshapedValues.entrySet() )
        {
            Ensemble ensembleSlice = Ensemble.of( events.getValue(), labels );
            Event<Ensemble> ensembleEvent = Event.of( events.getKey(), ensembleSlice );
            builder.addEvent( ensembleEvent );
        }

        return builder.build();
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
        else if (!dataProvider.getString( "variable_name" )
                              .equalsIgnoreCase( this.getDataSourceConfig().getVariable().getValue() ))
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

    private void ingest( wres.datamodel.time.TimeSeries<?> timeSeries )
            throws IngestException
    {
        TimeSeriesIngester timeSeriesIngester =
                this.createTimeSeriesIngester( this.getSystemSettings(),
                                               this.getDatabase(),
                                               this.getFeaturesCache(),
                                               this.getVariablesCache(),
                                               this.getEnsemblesCache(),
                                               this.getMeasurementUnitsCache(),
                                               this.getProjectConfig(),
                                               this.getDataSource(),
                                               this.getLockManager(),
                                               timeSeries );

        Future<List<IngestResult>> futureIngestResult =
                this.ingestSaverExecutor.submit(
                        timeSeriesIngester );
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
                                                 Variables variablesCache,
                                                 Ensembles ensemblesCache,
                                                 MeasurementUnits measurementUnitsCache,
                                                 ProjectConfig projectConfig,
                                                 DataSource dataSource,
                                                 DatabaseLockManager lockManager,
                                                 wres.datamodel.time.TimeSeries<?> timeSeries )
    {
        return TimeSeriesIngester.of( systemSettings,
                                      database,
                                      featuresCache,
                                      variablesCache,
                                      ensemblesCache,
                                      measurementUnitsCache,
                                      projectConfig,
                                      dataSource,
                                      lockManager,
                                      timeSeries );
    }
}
