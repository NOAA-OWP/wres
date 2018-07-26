package wres.io.reading.nwm;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.apache.commons.math3.util.Precision;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.DataSourceConfig;
import wres.io.concurrency.CopyExecutor;
import wres.io.concurrency.WRESRunnable;
import wres.io.concurrency.WRESRunnableException;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.Features;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.SourceDetails;
import wres.io.data.details.TimeSeries;
import wres.io.utilities.Database;
import wres.io.utilities.ScriptBuilder;
import wres.system.ProgressMonitor;
import wres.system.SystemSettings;
import wres.util.NetCDF;
import wres.util.Strings;

class VectorNWMValueSaver extends WRESRunnable
{

    private static final Logger LOGGER =
            LoggerFactory.getLogger( VectorNWMValueSaver.class );

    /**
     * Used as a key to tie variable and position identifiers to their
     * index in the NetCDF variable
     */
    private static class TimeSeriesIndexKey
    {
        TimeSeriesIndexKey(Integer variableID,
                                  String initializationDate,
                                  Integer ensembleID,
                                  Integer measurementUnitID)
        {
            this.variableID = variableID;
            this.initializationDate = initializationDate;
            this.ensembleID = ensembleID;
            this.measurementUnitID = measurementUnitID;
        }

        private final Integer variableID;
        private final String initializationDate;
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
    }

    // Used to map the mapping between netcdf vector indices to spatiovariable
    // ids from the database to the key for the series of values.
    //
    // If all short range channel_rt NWM forecasts from a certain date are
    // being ingested, that means that there will be ~432 different files,
    // split up into 24 groups of 18 files. Each group will get its own mapping
    // to indices since they will be unique to each group.
    //
    // A spatiovariable id is the identifier for the link between a variable
    // and a location. That id will be used to link a value for a variable
    // to a location.
    // In observations, the spatiovariable id is the variable position id,
    // with forecasts, it is the time series id
    private static Map<TimeSeriesIndexKey, Map<Integer, Integer>> indexMapping;

    // Keeps track of the IDs for all variables whose variable positions have
    // been generated. Only variables that are not recorded in this list
    // should have variable positions generated for them.
    private static final Set<Integer> addedVariables =
            Collections.synchronizedSet( new HashSet<Integer>() );

    // A collection of locks to use based on a series of values' context
    // This will allow the application to lock operations for a given
    // initialization date, variable, type of measurement,
    // and NWM grouping (short range, assim, etc) without blocking the other
    // NWM contexts (the short range stream flow for '2017-08-08 00:00:00'
    // won't block '2017-08-08 01:00:00')
    private static final Map<TimeSeriesIndexKey, Object> keyLocks = new ConcurrentHashMap<>(  );
    private static final Object KEY_LOCK = new Object();

    /**
     * Epsilon value used to test floating point equivalency
     */
    private static final double EPSILON = 0.0000001;

    /**
     * Gets a object to lock on based on the key for a time series
     * @param key The identifier for a set of circumstances that the NetCDF
     *            file belongs to
     * @return A sharable object to lock on
     */
    private static Object getKeyLock(TimeSeriesIndexKey key)
    {
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

    private static final String DELIMITER = "|";
    private static final String FORECAST_COLUMN_DEFINTIION =
            "(timeseries_id, lead, series_value)";


    private StringBuilder copyScript;
    private String copyHeader;
    private int copyCount = 0;

    private final Path filePath;
    private NetcdfFile source;
    private Integer lead;
    private Variable variable;
    private Integer variableID;
    private final Future<String> futureHash;
    private String hash;
    private final DataSourceConfig dataSourceConfig;
    private Integer measurementUnitID;
    private Integer sourceID;
    private Double missingValue;
    private boolean inChargeOfIngest;
    private Integer ensembleId;

    VectorNWMValueSaver( String filename,
                         Future<String> futureHash,
                         DataSourceConfig dataSourceConfig)
    {
        if (!Strings.hasValue( filename ))
        {
            throw new IllegalArgumentException("The passed filename is either null or empty.");
        }
        else if(futureHash == null)
        {
            throw new IllegalArgumentException( "No hash creation operation was passed to the ingestor." );
        }

        this.filePath = Paths.get(filename);
        this.futureHash = futureHash;
        this.dataSourceConfig = dataSourceConfig;
    }

    VectorNWMValueSaver (String filename,
                         String hash,
                         DataSourceConfig dataSourceConfig)
    {
        if (!Strings.hasValue( filename ))
        {
            throw new IllegalArgumentException("The passed filename is either null or empty.");
        }
        else if (!Strings.hasValue( hash ))
        {
            throw new IllegalArgumentException( "An empty or null hash was passed to the ingestor" );
        }

        this.filePath = Paths.get(filename);
        this.hash = hash;
        this.dataSourceConfig = dataSourceConfig;
        this.futureHash = null;
    }

    /**
     * Gets the result of the
     * @return The hash representing the source file
     * @throws IOException thrown if hashing was interrupted and if it
     * encountered an error during processing
     */
    private String getHash() throws IOException
    {
        if (this.hash == null)
        {
            try
            {
                this.hash = this.futureHash.get();
            }
            catch ( InterruptedException e )
            {
                String message = "The hashing process for the file '";
                message += this.filePath.toString();
                message += "' was interrupted and could not be completed.";
                LOGGER.warn( message );

                Thread.currentThread().interrupt();
            }
            catch ( ExecutionException e )
            {
                String message = "An error occurred while hashing the file '";
                message += this.filePath.toString();
                message += "'.";

                throw new IOException( message, e );
            }
        }
        return this.hash;
    }

    /**
     * Generates and returns the header used to copy data straight into the
     * database
     * @return The header for copy statements
     * @throws IOException Thrown if the lead time could not be retrieved
     * from the source file
     * @throws SQLException Thrown if an appropriate name for a partition
     * could not be retrieved from the database
     */
    private String getCopyHeader() throws IOException, SQLException
    {
        if (!Strings.hasValue( this.copyHeader ) && this.isForecast())
        {
            this.copyHeader = TimeSeries.getTimeSeriesValuePartition( this.getLead() );
            this.copyHeader += " ";
            this.copyHeader += VectorNWMValueSaver.FORECAST_COLUMN_DEFINTIION;
        }
        else if (!Strings.hasValue( this.copyHeader ))
        {
            this.copyHeader = "wres.Observation (" +
                                    "variablefeature_id, " +
                                    "observation_time, " +
                                    "observed_value, " +
                                    "measurementunit_id, " +
                                    "source_id " +
                              ")";
        }
        return this.copyHeader;
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
                    this.filePath.toString(),
                    NetCDF.getReferenceTime( this.getSource() ).toString(),
                    this.getLead(),
                    this.getHash()
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

                    // Now that ball is in our court we should put in cache
                    DataSources.put( sourceDetails );
                }
            }

            // Regardless of whether we were the ones or not, get it from cache
            this.sourceID = DataSources.getActiveSourceID( this.getHash() );

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

        // If this is a forecast file, we also need to attach the source
        // to the time series it belongs to, but only if this is the instance
        // in charge of ingest.
        if ( this.isForecast() && this.inChargeOfIngest )
        {
            this.addTimeSeriesSource();
        }

    }

    private void addTimeSeriesSource() throws IOException, SQLException
    {
        ScriptBuilder script = new ScriptBuilder(  );

        script.addLine("INSERT INTO wres.TimeSeriesSource (timeseries_id, source_id, lead)");
        script.addLine("SELECT TS.timeseries_id, ", this.sourceID, ", ", this.getLead());
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
    protected void execute ()
    {
        try
        {
            // Read and save the data from the NetCDF file
            this.read();

            LOGGER.debug("Finished Parsing '{}'", this.filePath);
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

        // If the object used to create the copy statement doesn't exist,
        // create it
        if ( this.copyScript == null)
        {
            this.copyScript = new StringBuilder(  );
        }

        // If this is a forecast, we need to link the lead time, the value,
        // and the id for the appropriate time series together
        if (this.isForecast())
        {
            this.copyScript.append( spatioVariableID )
                           .append( DELIMITER )
                           .append( this.getLead() )
                           .append( DELIMITER );

            // If the value is null, the value to copy should be '\N', which
            // postgresql recognizes as null in the copy statement.
            if (value == null)
            {
                this.copyScript.append("\\N");
            }
            else
            {
                this.copyScript.append(value);
            }

            this.copyScript.append( NEWLINE );
        }
        else
        {
            // If this is an observation, we want to link the location for the
            // variable to the time that the value is valid, the value itself,
            // the unit it was measured in, and the id of the source file
            this.copyScript.append( spatioVariableID )
                           .append( DELIMITER )
                           .append( NetCDF.getTime( this.getSource() ) )
                           .append( DELIMITER );

            // If the value is null, the value to copy should be '\N', which
            // postgresql recognizes as null in the copy statement.
            if (value == null)
            {
                this.copyScript.append("\\N");
            }
            else
            {
                this.copyScript.append(value);
            }

            this.copyScript.append( DELIMITER )
                           .append(this.getMeasurementUnitID())
                           .append( DELIMITER )
                           .append( this.getSourceID() )
                           .append( NEWLINE );
        }

        // Increase the count of queued values
        this.copyCount++;

        // If the number of queued values reaches the maximum number, save
        // everything
        if ( this.copyCount >= SystemSettings.getMaximumCopies())
        {
            this.saveValues();
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
        return ConfigHelper.isForecast( this.dataSourceConfig );
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
     * Sends all queued data to the database
     * @throws IOException Thrown if communication with the source file failed
     * @throws SQLException Thrown if communication with the database failed
     */
    private void saveValues() throws IOException, SQLException
    {
        // Only attempt to save if there is at least one value to copy
        if (this.copyCount > 0)
        {
            // Create the copy runnable with the data to copy and the header
            // appropriate to either the Observation table or the
            // forecast value table
            CopyExecutor copier = new CopyExecutor( this.getCopyHeader(),
                                                    this.copyScript.toString(),
                                                    DELIMITER );

            // TODO: If we want to only update the ProgressMonitor for files, remove these handlers
            // Tell the copier to increase the number representing the
            // total number of operations to perform when the thread starts.
            // It is debatable whether we should increase the number in this
            // thread or in the thread operating on the actual database copy
            // statement
            copier.setOnRun( ProgressMonitor.onThreadStartHandler() );

            // Tell the copier to inform the ProgressMonitor that work has been
            // completed when the thread has finished
            copier.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

            // Send the copier to the Database handler's task queue and add
            // the resulting future to our list of copy operations
            Database.ingest( copier );

            // Reset the values to copy
            this.copyScript = new StringBuilder(  );

            // Reset the count of values to copy
            this.copyCount = 0;
        }
    }

    /**
     * @return The NetCDF variable that needs to be read
     * @throws IOException Thrown if communication with the source file failed
     */
    private Variable getVariable() throws IOException {
        if (this.variable == null)
        {
            this.variable = NetCDF.getVariable(this.getSource(),
                                               this.dataSourceConfig
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
        // Build a script that creates a new time series for each valid vector
        // position that doesn't already exist
        ScriptBuilder script = new ScriptBuilder(  );
        script.addLine("INSERT INTO wres.TimeSeries (");
        script.addTab().addLine("variablefeature_id,");
        script.addTab().addLine("ensemble_id,");
        script.addTab().addLine("measurementunit_id,");
        script.addTab().addLine("initialization_date");
        script.addLine(")");
        script.addLine("SELECT VF.variablefeature_id,");
        script.addTab().addLine(this.getEnsembleID(), ",");
        script.addTab().addLine(this.getMeasurementUnitID(), "," );
        script.addTab().addLine("'", NetCDF.getReferenceTime( this.getSource() ), "'");
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
        String initializationTime = "";
        int ensembleID = 0;
        int measurementUnitID = 0;

        // If we are reading a forecast, we need to hold unique sets of IDs
        // based on variable, initialization, ensemble, and measurement unit.
        // For observations, we only need one based on variable. We want the
        // extra fields to only be populated uniquely if this is a forecast
        if (this.isForecast())
        {
            initializationTime = NetCDF.getReferenceTime( this.getSource() ).toString();
            ensembleID = this.getEnsembleID();
            measurementUnitID = this.getMeasurementUnitID();
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
        if ( VectorNWMValueSaver.indexMapping == null
             || !VectorNWMValueSaver.indexMapping.containsKey( key ) )
        {
            // Wait and lock down processing to ensure work isn't duplicated
            synchronized ( VectorNWMValueSaver.getKeyLock( key ) )
            {
                // Double check to make sure the necessary work wasn't done
                // while waiting for the lock
                if ( VectorNWMValueSaver.indexMapping == null
                     || !VectorNWMValueSaver.indexMapping.containsKey( key ) )
                {
                    // Add all missing locations for variables
                    this.addVariableFeatures();

                    String keyLabel = "comid";
                    String valueLabel;
                    ScriptBuilder script = new ScriptBuilder(  );

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
                        script.addTab(   3   ).addLine("WHERE S.output_time = '", NetCDF.getReferenceTime( this.getSource() ), "'");
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

                    // If a map hasn't been created yet, create it
                    if ( VectorNWMValueSaver.indexMapping == null )
                    {
                        VectorNWMValueSaver.indexMapping =
                                new ConcurrentHashMap<>();
                    }

                    // Create a map specific to this set of circumstances
                    Map<Integer, Integer> variableComids =
                            new TreeMap<>();

                    script.consume(
                            results -> variableComids.put(results.getInt( keyLabel),
                                                          results.getInt(valueLabel))
                    );

                    // Add the populated map to the overall collection
                    VectorNWMValueSaver.indexMapping.put( key,
                                                          getFeatureIndexMap( variableComids ) );
                }
            }
        }

        // Retrieve the custom tailored index mapping for these circumstances
        return VectorNWMValueSaver.indexMapping.get( key );
    }

    private Map<Integer, Integer> getFeatureIndexMap(Map<Integer, Integer> variableComids) throws IOException
    {
        Variable features = NetCDF.getVectorCoordinateVariable( this.getSource() );
        Map<Integer, Integer> featureIndexMap = new ConcurrentHashMap<>( variableComids.size() );

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
    private int getLead() throws IOException
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
                                .replaceAll( "^https:/", "https://" ));
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

        // Ensure that metadata for this file is added linked to the appropriate
        // time series
        this.addSource();

        if ( !this.inChargeOfIngest )
        {
            LOGGER.debug( "This VectorNWMValueSaver yields for source {}",
                          this.hash );
            return;
        }

        // Read all of the values from the NetCDF source at once
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
                    value = (value * scaleFactor) + offset;
                }

                // send it to be saved
                this.addValuesToSave(positionIndex, value);
            }
        }

        // Add any left over values to the queue to be saved
        saveValues();
    }

    @Override
    protected Logger getLogger() {
        return LOGGER;
    }
}
