package wres.io.reading.nwm;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import javax.validation.constraints.NotNull;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.ProjectConfig;
import wres.datamodel.metadata.TimeScale;
import wres.io.concurrency.WRESCallable;
import wres.io.concurrency.WRESRunnableException;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceCompletedDetails;
import wres.io.data.details.SourceDetails;
import wres.io.reading.DataSource;
import wres.io.reading.IngestResult;
import wres.io.reading.IngestedValues;
import wres.io.reading.SourceCompleter;
import wres.io.utilities.DataScripter;
import wres.system.DatabaseLockManager;
import wres.util.NetCDF;
import wres.util.Strings;
import wres.util.TimeHelper;

class VectorNWMValueSaver extends WRESCallable<List<IngestResult>>
{

    private static final Logger LOGGER =
            LoggerFactory.getLogger( VectorNWMValueSaver.class );

    /**
     * Used as a key to tie variable and position identifiers to their
     * index in the NetCDF variable
     */
    private static class TimeSeriesIndexKey implements Comparable<TimeSeriesIndexKey>
    {
        TimeSeriesIndexKey(Integer variableID,
                                  Instant initializationDate,
                                  Integer ensembleID,
                                  Integer measurementUnitID)
        {
            this.variableID = variableID;
            this.initializationDate = initializationDate;
            this.ensembleID = ensembleID;
            this.measurementUnitID = measurementUnitID;
        }

        private final Integer variableID;
        private final Instant initializationDate;
        private final Integer ensembleID;
        private final Integer measurementUnitID;

        @Override
        public boolean equals( Object obj )
        {
            return obj instanceof TimeSeriesIndexKey && this.hashCode() == obj.hashCode();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( this.variableID,
                                 this.initializationDate,
                                 this.ensembleID,
                                 this.measurementUnitID );
        }

        @Override
        public String toString()
        {
            return "Variable ID: " + String.valueOf( this.variableID ) +
                   ", Initialization Date: '" + this.initializationDate +
                   "', Ensemble ID: " + this.ensembleID +
                   ", Measurement unit ID: " + this.measurementUnitID;
        }

        @Override
        public int compareTo( TimeSeriesIndexKey other )
        {
            int comparison = Integer.compare( this.variableID, other.variableID );

            if (comparison == 0)
            {
                comparison = this.initializationDate.compareTo( other.initializationDate );
            }

            if (comparison == 0)
            {
                comparison = Integer.compare( this.ensembleID, other.ensembleID );
            }

            if (comparison == 0)
            {
                comparison = Integer.compare( this.measurementUnitID, other.measurementUnitID );
            }

            return comparison;
        }
    }

    /**
     * Used to map the mapping between netcdf vector indices to spatiovariable
     * ids from the database to the key for the series of values.
     * <p>
     * If all short range channel_rt NWM forecasts from a certain date are
     * being ingested, that means that there will be ~432 different files,
     * split up into 24 groups of 18 files. Each group will get its own mapping
     * to indices since they will be unique to each group.
     * </p>
     * <p>
     * A spatiovariable id is the identifier for the link between a variable
     * and a location. That id will be used to link a value for a variable
     * to a location.
     * In observations, the spatiovariable id is the variable position id,
     * with forecasts, it is the time series id
     * </p>
     * <pre>
     * TimeSeriesIndexKey:1 -> comid:1 -> spatiovariable_id:1
     *                      -> comid:2 -> spatiovariable_id:2
     * TimeSeriesIndexKey:2 -> comid:1 -> spatiovariable_id:3
     *                      -> comid:2 -> spatiovariable_id:4
     * TimeSeriesIndexKey:3 -> comid:1 -> spatiovariable_id:5
     *                      -> comid:2 -> spatiovariable_id:6
     * </pre>
     **/
    private static final Map<TimeSeriesIndexKey, Map<Integer, Integer>> indexMapping = new ConcurrentHashMap<>(  );

    /**
     * Keeps track of the IDs for all variables whose variable positions have
     * been generated. Only variables that are not recorded in this list
     * should have variable positions generated for them.
     **/
    private static final Set<Integer> addedVariables =
            Collections.synchronizedSet( new HashSet<>() );

    /**
     * A collection of locks to use based on a series of values' context
     * This will allow the application to lock operations for a given
     * initialization date, variable, type of measurement,
     * and NWM grouping (short range, assim, etc) without blocking the other
     * NWM contexts (the short range stream flow for '2017-08-08 00:00:00'
     * won't block '2017-08-08 01:00:00')
     **/
    private static final Map<TimeSeriesIndexKey, Object> keyLocks = new ConcurrentHashMap<>(  );
    private static final Object KEY_LOCK = new Object();

    /**
     * Epsilon value used to test floating point equivalency
     */
    private static final double EPSILON = 0.0000001;

    /**
     * The time scale of the NWM data is currently unknown. When the NWM
     * data is properly defined, the defaults of missing for both the 
     * {@link #SCALE_PERIOD} and {@link #SCALE_FUNCTION} should be overridden.
     * See discussion in #55216.
     */
    private static final Duration SCALE_PERIOD = null;

    /**
     * The time scale of the NWM data is currently unknown. When the NWM
     * data is properly defined, the defaults of missing for both the 
     * {@link #SCALE_PERIOD} and {@link #SCALE_FUNCTION} should be overridden.
     * See discussion in #55216.
     */
    private static final TimeScale.TimeScaleFunction SCALE_FUNCTION = TimeScale.TimeScaleFunction.UNKNOWN;

    /**
     * Gets a object to lock on based on the key for a time series
     * @param key The identifier for a set of circumstances that the NetCDF
     *            file belongs to
     * @return A sharable object to lock on
     */
    private static Object getKeyLock(TimeSeriesIndexKey key)
    {
        // TODO: Is a reentrant lock more appropriate here since there will be a lock per key?
        Object lock;
        synchronized ( KEY_LOCK )
        {
            // If no lock has been created, add one so it can be shared
            if (!keyLocks.containsKey( key ))
            {
                keyLocks.put( key, new Object() );
            }
            lock = keyLocks.get( key );
        }
        return lock;
    }

    private final ProjectConfig projectConfig;
    private final Path filePath;
    private NetcdfFile source;
    private final DataSource dataSource;
    private final DatabaseLockManager lockManager;
    private final Set<Pair<CountDownLatch, CountDownLatch>> latches = new HashSet<>();

    private Duration lead;
    private Variable variable;
    private Integer variableID;
    private final String hash;
    private Integer measurementUnitID;
    private Integer sourceID;
    private Double missingValue;
    private boolean inChargeOfIngest;
    private Integer ensembleId;

    VectorNWMValueSaver( ProjectConfig projectConfig,
                         DataSource dataSource,
                         String hash,
                         DatabaseLockManager lockManager )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( dataSource );
        Objects.requireNonNull( lockManager );

        if (!Strings.hasValue( hash ))
        {
            throw new IllegalArgumentException( "An empty or null hash was passed to the ingestor" );
        }

        this.projectConfig = projectConfig;
        this.filePath = Paths.get( dataSource.getUri() );
        this.hash = hash;
        this.dataSource = dataSource;
        this.lockManager = lockManager;
    }

    /**
     * @return The id of the measurement unit for the variable
     * @throws IOException Thrown if the variable could not be found on the source file
     * @throws SQLException Thrown if an error occurred while interacting with the database
     */
    private Integer getMeasurementUnitID() throws IOException, SQLException
    {
        if (this.measurementUnitID == null)
        {
            this.measurementUnitID = MeasurementUnits.getMeasurementUnitID( this.getVariable().getUnitsString() );
        }
        return measurementUnitID;
    }
    
    /**
     * Returns the period associated with the time scale of the data.
     * 
     * TODO: return something other than the static default, once the time scale
     * of the NWM data can be properly determined from the source.
     * 
     * @return the period associated with the time scale, which may be null
     */
    
    private Duration getTimeScalePeriod()
    {
        return VectorNWMValueSaver.SCALE_PERIOD;
    }
    
    /**
     * Returns the function associated with the time scale of the data.
     * 
     * TODO: return something other than the static default, once the time scale
     * of the NWM data can be properly determined from the source.
     * 
     * @return the function associated with the time scale, which may be null
     */
    
    private TimeScale.TimeScaleFunction getTimeScaleFunction()
    {
        return VectorNWMValueSaver.SCALE_FUNCTION;
    }
    
    /**
     * Side effect: sets inChargeOfIngest, a flag to say whether to save values
     * @return The id for the source file in the database
     * @throws IOException Thrown if communication with the source file failed
     * @throws SQLException Thrown if an error occurred while accessing the database
     */
    private Integer getSourceID() throws IOException, SQLException
    {
        if (this.sourceID == null)
        {
            // TODO: Modify the cache to do this work
            SourceDetails.SourceKey sourceKey = new SourceDetails.SourceKey(
                    this.filePath.toUri(),
                    NetCDF.getReferenceTime( this.getSource() ).toString(),
                    TimeHelper.durationToLead(this.getLead()),
                    this.hash
            );

            // Ask the cache "do you have this source?"
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

                    // We should mark that the ingest is ongoing. #50933
                    this.lockManager.lockSource( sourceDetails.getId() );

                    // Now that ball is in our court we should put in cache
                    DataSources.put( sourceDetails );
                }
            }

            // Regardless of whether we were the ones or not, get it from cache
            this.sourceID = DataSources.getActiveSourceID( this.hash );

            // Mark whether this reader is the one to perform ingest or yield.
            this.inChargeOfIngest = wasThisReaderTheOneThatInserted;
        }
        return this.sourceID;
    }

    /**
     * Tell the database to add data about the source file to the project if
     * it is not already there
     * @throws IOException Thrown if communication with the source file failed
     * @throws SQLException Thrown if communication with the database failed
     */
    private void addSource() throws IOException, SQLException
    {
        // Get the ID for the source file
        this.getSourceID();

    }

    private void addTimeSeriesSource() throws IOException, SQLException
    {
        DataScripter script = new DataScripter(  );

        script.addLine("INSERT INTO wres.TimeSeriesSource (timeseries_id, source_id, lead)");
        script.addLine("SELECT TS.timeseries_id, ", this.sourceID, ", ", TimeHelper.durationToLead( this.getLead()));
        script.addLine("FROM wres.TimeSeries TS");
        script.addLine("INNER JOIN wres.VariableFeature VF");
        script.addTab().addLine("ON VF.variablefeature_id = TS.variablefeature_id");
        script.addLine("WHERE TS.ensemble_id = ", this.getEnsembleID());
        script.addTab().addLine("AND TS.initialization_date = '", NetCDF.getReferenceTime( this.getSource() ), "'");
        script.addTab().addLine("AND TS.measurementunit_id = ", this.getMeasurementUnitID());
        script.addTab().addLine("AND VF.variable_id = ", this.getVariableID());
        script.addTab().addLine("AND NOT EXISTS (");
        script.addTab(  2  ).addLine("SELECT 1");
        script.addTab(  2  ).addLine("FROM wres.TimeSeriesSource TSS");
        script.addTab(  2  ).addLine("WHERE TSS.source_id = ", this.sourceID);
        script.addTab(   3   ).addLine("AND TSS.timeseries_id = TS.timeseries_id");
        script.addTab(  2  ).add(");");

        // Attach the source to its time series
        script.execute();
    }

    @Override
    protected List<IngestResult> execute ()
    {
        try
        {
            // Read and save the data from the NetCDF file
            this.read();

            LOGGER.debug("Finished Parsing '{}'", this.filePath);

            SourceCompletedDetails completedDetails =
                    new SourceCompletedDetails( this.sourceID );
            boolean completedIngest;

            if ( this.inChargeOfIngest )
            {
                SourceCompleter sourceCompleter = new SourceCompleter( this.sourceID,
                                                                       this.lockManager );
                // Unsafe publication?
                sourceCompleter.complete( this.latches );
                completedIngest = true;
            }
            else
            {
                completedIngest = completedDetails.wasCompleted();
            }

            return IngestResult.singleItemListFrom( this.projectConfig,
                                                    this.dataSource,
                                                    this.hash,
                                                    this.inChargeOfIngest,
                                                    !completedIngest );
        }
        catch (SQLException | IOException e )
        {
            String message = "Failed to read or save data from an NWM file.";
            throw new WRESRunnableException( message, e );
        }
        finally
        {
            // Like all file IO operations, we need to attempt to close out
            // our file descriptor
            if (this.source != null)
            {
                // Attempt to close the NetCDF file
                try
                {
                    this.source.close();
                }
                catch (IOException e)
                {
                    // Exception on close should not affect primary outputs.
                    LOGGER.warn("Could not close the NetCDF file: '{}'",
                                 this.filePath.toString(), e );
                }
            }
        }
    }

    /**
     * Add the read value to the queue of values to send to the database
     * @param spatioVariableID The id that ties the value to its locality and
     *                         variable. This will be the timeseries_id for
     *                         forecasts and the variablefeature_id for
     *                         observations
     * @param value The read value
     * @throws IOException Thrown if communication with the source file failed
     * @throws SQLException Thrown if database operations could not complete
     */
    private void addValuesToSave(Integer spatioVariableID, Double value)
            throws IOException, SQLException
    {
        // If the value that needs to be saved is marked as filler, skip it
        if (!measurementIsValid(value))
        {
            return;
        }

        // Synchronizer/handle that signifies "one waiting" left, "done" right.
        Pair<CountDownLatch,CountDownLatch> synchronizer;

        // If this is a forecast, we need to link the lead time, the value,
        // and the id for the appropriate time series together
        if (this.isForecast())
        {
            synchronizer = IngestedValues.addTimeSeriesValue( spatioVariableID, TimeHelper.durationToLead(this.getLead()), value );
            this.latches.add( synchronizer );
        }
        else
        {
            synchronizer = IngestedValues.observed( value )
                                         .measuredIn( this.getMeasurementUnitID() )
                                         .at(NetCDF.getTime(this.getSource()))
                                         .inSource( this.getSourceID() )
                                         .forVariableAndFeatureID( spatioVariableID )
                                         .scaleOf( this.getTimeScalePeriod() )
                                         .scaledBy( this.getTimeScaleFunction() )
                                         .add();
            this.latches.add( synchronizer );
        }
    }

    /**
     * @return The value representing a filler value (i.e. null)
     */
    private Double getMissingValue() throws IOException
    {
        // Attempt to get the missing value from the variable
        if (this.missingValue == null)
        {
            try
            {
                this.missingValue = NetCDF.getMissingValue(this.getVariable());
            }
            catch (IOException e)
            {
                String message = "The variable could not be retrieved from the NetCDF source file.";
                throw new IOException( message, e );
            }
        }

        // If no variable missing value was found, check to see if one exists
        // globally
        if (this.missingValue == null)
        {
            try
            {
                this.missingValue = NetCDF.getGlobalMissingValue(this.getSource());
            }
            catch (IOException e)
            {
                String message = "The source NetCDF file could not be loaded.";
                throw new IOException( message, e );
            }
        }
        else
        {
            // The actual values are stored as integers and the scale factor
            // is used to convert that value to the intended double. Since a
            // value was found, scale it
            this.missingValue *= NetCDF.getScaleFactor( this.getVariable() );
        }

        // Since nothing was found, set it to the smallest number possible
        if (this.missingValue == null)
        {
            this.missingValue = -1 * Double.MAX_VALUE;
        }

        return this.missingValue;
    }

    /**
     * @return Whether or not the data is intended as forecast data
     */
    private boolean isForecast()
    {
        return ConfigHelper.isForecast( this.dataSource.getContext() );
    }


    /**
     * Determines if a given value is valid value, not filler
     * @param measurement The value to test
     * @return Whether or not the value is valid
     * @throws IOException Thrown if the "missing" or "filler" value could not be found
     */
    private boolean measurementIsValid(Double measurement) throws IOException
    {
        return this.getMissingValue() == null ||
               !String.valueOf(measurement)
                      .equalsIgnoreCase(String.valueOf(this.getMissingValue()));
    }

    /**
     * @return The NetCDF variable that needs to be read
     * @throws IOException Thrown if communication with the source file failed
     */
    private Variable getVariable() throws IOException {
        if (this.variable == null)
        {
            this.variable = NetCDF.getVariable(this.getSource(),
                                               this.dataSource
                                                       .getContext()
                                                       .getVariable()
                                                       .getValue());
        }
        return this.variable;
    }

    /**
     * @return The ID of the variable
     * @throws IOException Thrown if an ID could not be found or generated
     * generated in the database
     */
    private Integer getVariableID() throws IOException
    {
        if (this.variableID == null)
        {
            try
            {
                this.variableID = Variables.getVariableID(this.getVariable().getShortName());
            }
            catch (SQLException e)
            {
                String message = "A variable ID for '" +
                                 this.getVariable().getShortName() +
                                 "' could not be retrieved from the database.";
                throw new IOException( message, e );
            }
        }
        return this.variableID;
    }

    /**
     * Adds all valid locations for this variable to the database
     * @throws IOException Thrown if information about the variable could not be
     * read and sent to the database
     * @throws SQLException Thrown if communication with the database failed
     */
    private void addVariableFeatures() throws IOException, SQLException
    {
        synchronized ( VectorNWMValueSaver.addedVariables)
        {
            // Only add variable positions for variables that haven't had positions
            // added in this session. If we don't do this, each file will attempt
            // to create their own variable positions. We only want this done
            // once per variable.
            if ( !VectorNWMValueSaver.addedVariables.contains( this.getVariableID() ) )
            {
                Features.addNHDPlusVariableFeatures(this.getVariableID());
                VectorNWMValueSaver.addedVariables.add( this.getVariableID() );
            }
        }
    }

    /**
     * Adds all time series information about this data to the database
     * @throws IOException Thrown if information could not be retrieved
     * from the source file
     * @throws SQLException Thrown if communication with the database failed
     */
    private void addTimeSeries() throws IOException, SQLException
    {
        // TODO: when the scale_period is known for NWM data, 
        // adapt this script to convert the non-null value to 
        // an appropriate type for ingest. The scale_function
        // is non-null but UNKNOWN, so that is already handled.
        // See #55216
        
        // Build a script that creates a new time series for each valid vector
        // position that doesn't already exist
        DataScripter script = new DataScripter(  );
        script.addLine("INSERT INTO wres.TimeSeries (");
        script.addTab().addLine("variablefeature_id,");
        script.addTab().addLine("ensemble_id,");
        script.addTab().addLine("measurementunit_id,");
        script.addTab().addLine("initialization_date,");
        script.addTab().addLine("scale_period,");
        script.addTab().addLine("scale_function");
        script.addLine(")");
        script.addLine("SELECT VF.variablefeature_id,");
        script.addTab().addLine(this.getEnsembleID(), ",");
        script.addTab().addLine(this.getMeasurementUnitID(), "," );
        script.addTab().addLine("'", NetCDF.getReferenceTime( this.getSource() ), "',");
        script.addTab().addLine(this.getTimeScalePeriod(), ",");
        script.addTab().addLine("'", this.getTimeScaleFunction(), "'");
        script.addLine("FROM wres.VariableFeature VF");
        script.addLine("INNER JOIN wres.Feature F");
        script.addTab().addLine("ON F.feature_id = VF.feature_id");
        script.addLine("WHERE variable_id = ", this.getVariableID());
        script.addTab(  2  ).addLine("AND F.comid IS NOT NULL");
        script.addTab(  2  ).addLine("AND F.comid != -999");
        script.addTab(  2  ).addLine("AND NOT EXISTS (");
        script.addTab(   3   ).addLine("SELECT 1");
        script.addTab(   3   ).addLine("FROM wres.TimeSeries TS");
        script.addTab(   3   ).addLine("WHERE TS.variablefeature_id = VF.variablefeature_id");
        script.addTab(    4    ).addLine("AND TS.ensemble_id = ", this.getEnsembleID());
        script.addTab(    4    ).addLine("AND TS.initialization_date = '", NetCDF.getReferenceTime( this.getSource() ), "'");
        script.addTab(   3   ).add(");");
        script.execute();
        this.addSource();
    }

    /**
     * @return the ID for the ensemble for this data
     * @throws SQLException Thrown if the ID could not be retrieved from the
     * database
     * @throws IOException Thrown if ensemble information could not be retrieved
     * from the source file
     */
    private int getEnsembleID() throws SQLException, IOException
    {
        if (this.ensembleId == null)
        {
            this.ensembleId = Ensembles.getEnsembleID( NetCDF.getEnsemble( this.getSource() ) );
        }
        return this.ensembleId;
    }

    /**
     * Retrieves a mapping between the location index in the source file and
     * the ID of the corresponding spatial, variable, and/or ensemble id
     * <p>
     *     <b>Note:</b> For forecasts, the ID of the appropriate time series
     *     is mapped to the netcdf location index, while the position of the
     *     variable is mapped to the netcdf location index for observations.
     *     The difference is due to forecasts needing to be linked to
     *     the forecast initialization date and ensemble, along with the position
     *     of the variable.
     * </p>
     *<p>
     *     Using the resulting index mapping, we can find the spatial variable
     *     index by inputting the index of the read NetCDF variable.
     *     So, if I encounter index 93832 in the NetCDF array, I can
     *     discover that that value should be saved with timeseries_id = 83
     *</p>
     * @return A mapping between the netcdf location index and the id of its
     * spatial-variable identifier
     * @throws IOException Thrown if communication with the source file failed
     * @throws SQLException Thrown if communication with the database failed
     */
    @NotNull
    private Map<Integer, Integer> getIndexMapping() throws IOException, SQLException
    {
        Instant initializationTime = Instant.MIN;
        final int ensembleID;
        int measurementUnitID = 0;

        // If we are reading a forecast, we need to hold unique sets of IDs
        // based on variable, initialization, ensemble, and measurement unit.
        // For observations, we only need one based on variable. We want the
        // extra fields to only be populated uniquely if this is a forecast
        if (this.isForecast())
        {
            initializationTime = NetCDF.getReferenceTime( this.getSource() );
            ensembleID = this.getEnsembleID();
            measurementUnitID = this.getMeasurementUnitID();
        }
        else
        {
            // If the values aren't forecasts, the data will be in wres.Observation and won't have a normal ensembleID
            ensembleID = -1;
        }

        // A key is built to ensure proper index information for the variable,
        // forecast initialization, ensemble, and measurement unit is retrieved
        // and/or generated. During forecast ingest, many NetCDF files may be
        // read and their data may need to be linked to different sets of time
        // series ids, while many others will need to be shared. We could
        // read >200 files that will need all the same IDs, while the next set
        // of NetCDF files will need completely new IDs.
        TimeSeriesIndexKey key = new TimeSeriesIndexKey( this.getVariableID(),
                                                         initializationTime,
                                                         ensembleID,
                                                         measurementUnitID);

        // If there is no mapping yet or there is no mapping for these
        // circumstances...
        if ( !VectorNWMValueSaver.indexMapping.containsKey( key ) )
        {
            // Wait and lock down processing to ensure work isn't duplicated
            synchronized ( VectorNWMValueSaver.getKeyLock( key ) )
            {
                // Double check to make sure the necessary work wasn't done
                // while waiting for the lock
                if ( !VectorNWMValueSaver.indexMapping.containsKey( key ) )
                {
                    // Add all missing locations for variables
                    this.addVariableFeatures();

                    String keyLabel = "comid";
                    String valueLabel;
                    DataScripter script = new DataScripter(  );

                    if ( this.isForecast() )
                    {
                        // Add all time series that correspond with this data
                        this.addTimeSeries();

                        // Form a script that will retrieve the IDs for each
                        // valid location for each time series for this set
                        // of data
                        valueLabel = "timeseries_id";
                        script.addLine("SELECT F.", keyLabel, ", TS.", valueLabel);
                        script.addLine("FROM wres.TimeSeries TS");
                        script.addLine("INNER JOIN (");
                        script.addTab().addLine("SELECT VF.variablefeature_id, F.", keyLabel);
                        script.addTab().addLine("FROM wres.Feature F");
                        script.addTab().addLine("INNER JOIN wres.VariableFeature VF");
                        script.addTab(  2  ).addLine("ON VF.feature_id = F.feature_id");
                        script.addTab().addLine("WHERE F.comid IS NOT NULL");
                        script.addTab(  2  ).addLine("AND F.comid > 0");
                        script.addTab(  2  ).addLine("AND VF.variable_id = ", this.getVariableID());
                        script.addLine(") AS F");
                        script.addTab().addLine("ON F.variablefeature_id = TS.variablefeature_id");
                        script.addLine("WHERE TS.initialization_date = '", initializationTime, "'");
                        script.addTab().addLine("AND TS.ensemble_id = ", ensembleID);
                        script.addTab().addLine("AND EXISTS (");
                        script.addTab(  2  ).addLine("SELECT 1");
                        script.addTab(  2  ).addLine("FROM wres.TimeSeriesSource TSS");
                        script.addTab(  2  ).addLine("INNER JOIN (");
                        script.addTab(   3   ).addLine("SELECT S.source_id");
                        script.addTab(   3   ).addLine("FROM wres.Source S");
                        script.addTab(   3   ).addLine("WHERE S.output_time = '", initializationTime, "'");
                        script.addTab(  2  ).addLine(") AS S");
                        script.addTab(   3   ).addLine("ON S.source_id = TSS.source_id");
                        script.addTab(  2  ).addLine("LEFT OUTER JOIN wres.ProjectSource PS");
                        script.addTab(   3   ).addLine("ON PS.source_id = TSS.source_id");
                        script.addTab(  2  ).addLine("WHERE NOT EXISTS (");
                        script.addTab(    4    ).addLine("SELECT 1");
                        script.addTab(    4    ).addLine("FROM wres.ProjectSource PS");
                        script.addTab(    4    ).addLine("WHERE PS.source_id = S.source_id");
                        script.addTab(   3   ).addLine(")");
                        script.addTab(   3   ).addLine("AND TSS.timeseries_id = TS.timeseries_id");
                        script.addTab().addLine(");");
                    }
                    else
                    {
                        // Form a script that will map each variable position
                        // to each valid location for the variable in question
                        valueLabel = "variablefeature_id";
                        script.addLine("SELECT ", keyLabel, ", ", valueLabel);
                        script.addLine("FROM wres.VariableFeature VF");
                        script.addLine("INNER JOIN wres.Feature F");
                        script.addTab().addLine("ON F.feature_id = VF.feature_id");
                        script.addLine("WHERE F.comid > 0");
                        script.addTab().addLine("AND VF.variable_id = ", this.getVariableID(), ";");
                    }

                    // Create a map specific to this set of circumstances
                    Map<Integer, Integer> variableComids =
                            new TreeMap<>();

                    script.consume(
                            results -> variableComids.put(results.getInt( keyLabel),
                                                          results.getInt(valueLabel))
                    );

                    // Add the populated map to the overall collection
                    // TODO: Find way to avoid calling "getFeatureIndexMap"; the value is going to be the same 99.9% of the time
                    VectorNWMValueSaver.indexMapping.put( key,
                                                          getFeatureIndexMap( variableComids ) );
                }
            }
        }

        // Retrieve the custom tailored index mapping for these circumstances
        return VectorNWMValueSaver.indexMapping.get( key );
    }

    /**
     * Generates a map between the indices in the variable in the netcdf file and the
     * spatiovariable ids that they correspond to
     * @param variableComids A mapping of the spatials ids for variables to the spatiovariable ids in the database
     * @return A mapping between array index and spatiovariable id
     * @throws IOException Thrown if the netcdf file could not be loaded
     * @throws IOException Thrown if the netcdf file could not be read
     */
    private Map<Integer, Integer> getFeatureIndexMap(Map<Integer, Integer> variableComids) throws IOException
    {
        Variable features = NetCDF.getVectorCoordinateVariable( this.getSource() );
        Map<Integer, Integer> featureIndexMap = new ConcurrentHashMap<>( variableComids.size() );

        // Read in all values from the variable; the cost of multiple reads from the disk outweighs
        // the cost of having these values in memory
        Array comids = features.read();

        for (int comidIndex = 0; comidIndex < comids.getSize(); ++comidIndex)
        {
            int comid = comids.getInt( comidIndex );
            if (variableComids.containsKey( comid ))
            {
                featureIndexMap.put( comidIndex, variableComids.get( comid ) );
            }
        }

        return featureIndexMap;
    }

    /**
     * @return The lead time relative to the initialization date for this data
     * @throws IOException Thrown if communication with the source file failed
     */
    private Duration getLead() throws IOException
    {
        if (this.lead == null)
        {
            this.lead = NetCDF.getLeadTime( this.getSource());
        }
        return this.lead;
    }

    /**
     * @return The open NetCDF file accessor
     * @throws IOException Thrown if the NetCDF file could not be accessed
     */
    private NetcdfFile getSource() throws IOException {
        if (this.source == null)
        {
            try
            {
                // The Path class will convert "http://whatever.com" to
                // "http:/whatever.com", so that needs to be reverted
                this.source = NetcdfFile.open(
                        filePath.toString()
                                .replaceAll( "^http:/", "http://" )
                                .replaceAll( "^https:/", "https://" )
                );
            }
            catch ( IOException e )
            {
                String message = "A file at: '"
                                 + this.filePath
                                 + "' could not be loaded as a NetCDF file.";
                throw new IOException( message, e );
            }
        }
        return this.source;
    }

    /**
     * Read and save all valid NetCDF vector data
     * @throws IOException Thrown if the source data could not be read
     * @throws SQLException Thrown if the data could not be saved to the database
     */
    private void read() throws IOException, SQLException
    {
        Variable var = this.getVariable();

        if ( !this.inChargeOfIngest )
        {
            LOGGER.debug( "This VectorNWMValueSaver yields for source {}",
                          this.hash );
            return;
        }

        // Ensure that metadata for this file is added linked to the appropriate
        // time series
        this.addSource();

        // Find the factor with which to scale all read values
        double scaleFactor = NetCDF.getScaleFactor(var);

        // Find the smallest possible value
        double minimumValue = NetCDF.getMinimumValue( var );

        // Find the largest possible value
        double maximumValue = NetCDF.getMaximumValue( var );

        // Find the offset for all values
        double offset = NetCDF.getAddOffset( var );

        // Get the mapping between the soon to be read array indices and the
        // IDs for its spatial and variable identifiers
        Map<Integer, Integer> variableIndices = this.getIndexMapping();

        // If this is a forecast file, we also need to attach the source
        // to the time series it belongs to, but only if this is the instance
        // in charge of ingest.
        if ( this.isForecast() )
        {
            this.addTimeSeriesSource();
        }

        // Read in all values from the variable; the cost of multiple reads from the disk outweighs
        // the cost of having these values in memory
        Array values = var.read();

        // Loop through each value in the array of stored values
        for (int index = 0; index < values.getSize(); ++index)
        {
            // If we have a mapping for the index to an identifier for its
            // location and variable, add the data to the queue to be saved to
            // the database with the appropriate scaling
            if (variableIndices.containsKey(index))
            {
                // Get the ID for the location and variable for this index
                Integer positionIndex = variableIndices.get(index);
                Double value = values.getDouble( index );

                if ( Precision.equals( value, this.getMissingValue(), EPSILON ) ||
                     value > maximumValue ||
                     value < minimumValue )
                {
                    value = null;
                }
                else
                {
                    // TODO: Double check to make sure that the CDM isn't already doing this
                    value = (value * scaleFactor) + offset;
                }

                // send it to be saved
                this.addValuesToSave(positionIndex, value);
            }
        }
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
