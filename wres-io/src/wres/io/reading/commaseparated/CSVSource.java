package wres.io.reading.commaseparated;

import java.io.IOException;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.ProjectConfig;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.TimeSeries;
import wres.io.reading.BasicSource;
import wres.io.reading.DataSource;
import wres.io.reading.IngestException;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.SourceCompleter;
import wres.io.utilities.DataProvider;
import wres.io.utilities.Database;
import wres.system.DatabaseLockManager;
import wres.system.SystemSettings;
import wres.util.LRUContainer;
import wres.util.Strings;
import wres.util.TimeHelper;

public class CSVSource extends BasicSource
{
    private static final Logger LOGGER = LoggerFactory.getLogger( CSVSource.class );

    // It's probably worth making this configurable
    private static final String DELIMITER = ",";

    private static final int TIME_SERIES_LIMIT = 60;

    private final SystemSettings systemSettings;
    private final Database database;
    private final DataSources dataSourcesCache;
    private final Features featuresCache;
    private final Variables variablesCache;
    private final Ensembles ensemblesCache;
    private final MeasurementUnits measurementUnitsCache;
    private final DatabaseLockManager lockManager;
    private final Set<Pair<CountDownLatch,CountDownLatch>> latches = new HashSet<>();
    private final Set<String> unconfiguredVariableNames = new HashSet<>( 1 );
    private SourceDetails sourceDetails;

    /**
     * A container holding referenced TimeSeries entries
     */
    private LRUContainer<TimeSeries> encounteredTimeSeries;

    /**
     * Constructor that sets the filename
     * @param systemSettings The system settings to use.
     * @param database The database to use.
     * @param dataSourcesCache The data sources cache to use.
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
                      DataSources dataSourcesCache,
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
        this.dataSourcesCache = dataSourcesCache;
        this.featuresCache = featuresCache;
        this.variablesCache = variablesCache;
        this.ensemblesCache = ensemblesCache;
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

    private DataSources getDataSourcesCache()
    {
        return this.dataSourcesCache;
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

    @Override
    protected List<IngestResult> saveObservation() throws IOException
    {
        try
        {
            this.sourceDetails = this.getSourceDetailsAndCacheThemIfAbsent();
        }
        catch ( SQLException e )
        {
            throw new IOException( "Metadata about the file at '" + this.getFilename() + "' could not be created.", e );
        }

        Database database = this.getDatabase();
        boolean sourceCompleted;

        if (sourceDetails.performedInsert())
        {
            try
            {
                lockManager.lockSource( sourceDetails.getId() );
            }
            catch ( SQLException se )
            {
                throw new IngestException( "Unable to lock to ingest source id "
                                           + sourceDetails.getId() + " named "
                                           + this.getFilename(), se );
            }

            DataProvider data;

            if (this.getSourceConfig().isHasHeader())
            {
                data = DataProvider.fromCSV( this.getFilename(), DELIMITER );
            }
            else
            {
                data = DataProvider.fromCSV(
                        this.getFilename(),
                        DELIMITER,
                        "value_date",
                        "variable_name",
                        "location",
                        "measurement_unit",
                        "value"
                );
            }

            this.parseObservations( data );
            SourceCompleter sourceCompleter = new SourceCompleter( this.getDatabase(),
                                                                   sourceDetails.getId(),
                                                                   this.lockManager );
            sourceCompleter.complete( this.latches );
            sourceCompleted = true;
        }
        else
        {
            sourceCompleted = this.wasCompleted( database,
                                                 this.sourceDetails );
        }

        if ( !this.unconfiguredVariableNames.isEmpty() )
        {
            LOGGER.warn( "The following variable names were encountered in observation csv data source from {} that were not configured in the project: {}",
                         this.getFilename(),
                         this.unconfiguredVariableNames );
        }

        return IngestResult.singleItemListFrom(
                this.getProjectConfig(),
                this.getDataSource(),
                this.sourceDetails.getId(),
                !sourceDetails.performedInsert(),
                !sourceCompleted
        );
    }

    @Override
    protected List<IngestResult> saveForecast() throws IOException
    {
        if (encounteredTimeSeries == null)
        {
            encounteredTimeSeries = new LRUContainer<>( TIME_SERIES_LIMIT );
        }

        try
        {
            this.sourceDetails = this.getSourceDetailsAndCacheThemIfAbsent();
        }
        catch ( SQLException e )
        {
            throw new IOException( "Metadata about the file at '" + this.getFilename() + "' could not be created.", e );
        }

        Database database = this.getDatabase();
        boolean sourceCompleted;

        if (sourceDetails.performedInsert())
        {
            try
            {
                lockManager.lockSource( sourceDetails.getId() );
            }
            catch ( SQLException se )
            {
                throw new IngestException( "Unable to lock to ingest source id "
                                           + sourceDetails.getId() + " named "
                                           + this.getFilename(), se );
            }

            DataProvider data;

            if (this.getSourceConfig().isHasHeader())
            {
                data = DataProvider.fromCSV( this.getFilename(), DELIMITER );
            }
            else
            {
                data = DataProvider.fromCSV(
                        this.getFilename(),
                        DELIMITER,
                        "start_date",
                        "value_date",
                        "variable_name",
                        "location",
                        "measurement_unit",
                        "value",
                        "ensemble_name",
                        "qualifier_id",
                        "ensemblemember_id"
                );
            }

            parseTimeSeries( data );
            SourceCompleter sourceCompleter = new SourceCompleter( this.getDatabase(),
                                                                   sourceDetails.getId(),
                                                                   this.lockManager );
            sourceCompleter.complete( this.latches );
            sourceCompleted = true;
        }
        else
        {
            sourceCompleted = this.wasCompleted( database,
                                                 this.sourceDetails );
        }

        if ( !this.unconfiguredVariableNames.isEmpty() )
        {
            LOGGER.warn( "The following variable names were encountered in forecast csv data source from {} that were not configured in the project: {}",
                         this.getFilename(),
                         this.unconfiguredVariableNames );
        }

        return IngestResult.singleItemListFrom(
                this.getProjectConfig(),
                this.getDataSource(),
                this.sourceDetails.getId(),
                !sourceDetails.performedInsert(),
                !sourceCompleted
        );
    }

    private void parseTimeSeries(final DataProvider data) throws IOException
    {
        TimeSeries currentTimeSeries;

        while (data.next())
        {
            try
            {
                this.validateDataProvider( data );

                Instant start = data.getInstant( "start_date" );
                Instant valueDate = data.getInstant( "value_date" );
                Double value = data.getDouble( "value" );
                int ensembleId = this.getEnsembleId( data );
                int lead = (int) TimeHelper.durationToLongUnits( Duration.between( start, valueDate ),
                                                                 TimeHelper.LEAD_RESOLUTION );

                currentTimeSeries = formTimeSeries( data, ensembleId);

                Pair<CountDownLatch,CountDownLatch> synchronizer =
                        IngestedValues.addTimeSeriesValue( this.getSystemSettings(),
                                                           this.getDatabase(),
                                                           currentTimeSeries.getTimeSeriesID(),
                                                           lead,
                                                           value );
                this.latches.add( synchronizer );
            }
            catch (SQLException e)
            {
                throw new IOException( "Metadata needed to save time series values could not be loaded.", e );
            }
        }
    }

    private int getEnsembleId(final DataProvider data) throws SQLException
    {
        String ensembleName = "default";
        String qualifierID = null;
        Integer ensembleMemberID = null;

        if (data.hasColumn("ensemble_name"))
        {
            ensembleName = data.getString("ensemble_name");
        }

        if (data.hasColumn("qualifier_id"))
        {
            qualifierID = data.getString("qualifier_id");
        }

        if (data.hasColumn("ensemblemember_id"))
        {
            ensembleMemberID = data.getInt("ensemblemember_id");
        }

        Ensembles ensembles = this.getEnsemblesCache();
        return ensembles.getEnsembleID(ensembleName, ensembleMemberID, qualifierID);
    }

    private void parseObservations(final DataProvider data) throws IOException
    {
        while (data.next())
        {
            this.validateDataProvider( data );

            Instant valueDate = data.getInstant( "value_date" );
            Double value = data.getDouble( "value" );

            String variable = data.getString( "variable_name" );
            String location = data.getString( "location" );
            String measurementUnit = data.getString( "measurement_unit" );
            Integer measurementUnitId;
            MeasurementUnits measurementUnits = this.getMeasurementUnitsCache();

            try
            {
                measurementUnitId = measurementUnits.getMeasurementUnitID( measurementUnit );
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not determine the ID for the measurement unit '" +
                                       measurementUnit + "'", e );
            }

            Integer variableFeatureId;
            Features features = this.getFeaturesCache();
            Variables variables = this.getVariablesCache();

            try
            {
                variableFeatureId = features.getVariableFeatureIDByLID(
                        location,
                        variables.getVariableID(variable)
                );
            }
            catch ( SQLException e )
            {
                throw new IOException( "Could not determine the metadata about where '" +
                                       location + "' and the variable '" + variable +
                                       "' intersect that are needed to save observations from " +
                                       this.getFilename(), e );
            }

            Pair<CountDownLatch,CountDownLatch> synchronizer =
                    IngestedValues.observed( value )
                                  .at(valueDate)
                                  .measuredIn( measurementUnitId )
                                  .forVariableAndFeatureID( variableFeatureId )
                                  .inSource( this.sourceDetails.getId() )
                                  .add( this.systemSettings, this.database );
            this.latches.add( synchronizer );
        }
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
        if ( ConfigHelper.isForecast( this.getDataSourceConfig() ))
        {
            hasColumn = dataProvider.hasColumn( "start_date" );

            if (!hasColumn)
            {
                valid = false;
                errorJoiner.add( "The provided csv is missing a 'start_date' column." );
            }
            else if (!Strings.hasValue( dataProvider.getString( "start_date" )))
            {
                errorJoiner.add("The provided csv is missing valid 'start_date' data.");
                valid = false;
            }
            else
            {
                try
                {
                    dataProvider.getInstant( "start_date" );
                }
                catch ( DateTimeParseException | ClassCastException e )
                {
                    errorJoiner.add("The provided csv has invalid data within the 'start_date' column.");
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

    private TimeSeries formTimeSeries(final DataProvider data, final int ensembleId) throws SQLException
    {
        final String variable = data.getString( "variable_name" );
        final String location = data.getString( "location" );
        final String measurementUnit = data.getString( "measurement_unit" );
        final String startDate = data.getInstant( "start_date" ).toString();

        Features features = this.getFeaturesCache();
        Variables variables = this.getVariablesCache();

        final Integer variableFeatureId = features.getVariableFeatureIDByLID(
                location,
                variables.getVariableID(variable)
        );

        TimeSeries timeSeries = this.encounteredTimeSeries.get(
                series -> series.getEnsembleId() == ensembleId &&
                          series.getInitializationDate().equals( startDate ) &&
                          series.getVariableFeatureID().equals( variableFeatureId )
        );

        if (timeSeries != null)
        {
            return timeSeries;
        }

        timeSeries = new TimeSeries(
                database,
                this.sourceDetails.getId(),
                startDate
        );

        timeSeries.setEnsembleID(ensembleId);

        MeasurementUnits measurementUnits = this.getMeasurementUnitsCache();
        timeSeries.setMeasurementUnitID(
                measurementUnits.getMeasurementUnitID(measurementUnit)
        );

        timeSeries.setVariableFeatureID( variableFeatureId );
        
        // Time scale information is missing by default        
        //timeSeries.setScalePeriod( 1 );

        this.encounteredTimeSeries.add(timeSeries);

        return timeSeries;
    }

    /**
     * Attempts to return the source details and adds them to the centralized application cache if they are absent.
     * 
     * @return the source details
     * @throws SQLException if the source details could not be created
     */

    private SourceDetails getSourceDetailsAndCacheThemIfAbsent() throws SQLException
    {
        // See: #72718 
        // Create a source key to check
        Instant sourceTime = Instant.now();
        SourceDetails.SourceKey sourceKey = new SourceDetails.SourceKey( this.getFilename(),
                                                                         sourceTime.toString(),
                                                                         null,
                                                                         this.getHash() );
        Database database = this.getDatabase();
        DataSources dataSources = this.getDataSourcesCache();

        // Is it in the application cache?
        boolean isInCache = dataSources.isCached( sourceKey );

        SourceDetails returnMe = null;

        // No, not in the cache, so create and add to the application cache
        if ( !isInCache )
        {
            returnMe = new SourceDetails( sourceKey );

            // Attempt to save the source details to the database
            returnMe.save( database );

            // If we saved, then we should update the application cache
            if ( returnMe.performedInsert() )
            {
                LOGGER.trace( "Could not find CSV source '{}' in the cache. Adding...", returnMe );

                // Cache
                dataSources.put( returnMe );

                LOGGER.trace( "Added CSV source '{}' to the cache.", returnMe );
            }
        }
        // Yes, it's in the application cache, so return from there
        else
        {
            returnMe = dataSources.get( this.getFilename(), sourceTime.toString(), null, this.getHash() );
        }

        return returnMe;
    }
    
    /**
     * Discover whether the source was completely ingested
     * @param sourceDetails the source to query
     * @throws IngestException when discovery fails due to SQLException
     * @return true if the source has been marked as completed, false otherwise
     */

    private boolean wasCompleted( Database database,
                                  SourceDetails sourceDetails )
            throws IngestException
    {
        SourceCompletedDetails completed = new SourceCompletedDetails( database,
                                                                       sourceDetails );

        try
        {
            return completed.wasCompleted();
        }
        catch ( SQLException se )
        {
            throw new IngestException( "Failed discover whether source "
                                       + sourceDetails + " was completed.",
                                       se );
        }
    }

    @Override
    protected Logger getLogger()
    {
        return CSVSource.LOGGER;
    }
}
