package wres.io.reading.ucar;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;

import wres.config.generated.DataSourceConfig;
import wres.io.concurrency.CopyExecutor;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.ConfigHelper;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.TimeSeries;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.NetCDF;
import wres.util.ProgressMonitor;
import wres.util.Strings;

@Internal(exclusivePackage = "wres.io")
class VectorNetCDFValueSaver extends WRESRunnable
{
    /**
     * Used as a key to tie variable and position identifiers to their
     * index in the NetCDF variable
     */
    private class TimeSeriesIndexKey
    {
        public TimeSeriesIndexKey(Integer variableID,
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
            return this.hashCode() == obj.hashCode();
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

    private static final Object VARIABLEPOSITION_LOCK = new Object();
    private static final String DELIMITER = "|";
    private static final String COLUMN_DEFINTIION = "(timeseries_id, lead, forecasted_value)";

    private final static Logger LOGGER = LoggerFactory.getLogger(VectorNetCDFValueSaver.class);
    private StringBuilder copyScript;
    private String copyHeader;
    private int copyCount = 0;

    private final Path filePath;
    private NetcdfFile source;
    private final Stack<Future<?>> operations = new Stack<>();
    private Integer lead;
    private Variable variable;
    private Integer variableID;
    private String variablePositionPartitionName;
    private final Future<String> futureHash;
    private String hash;
    private final ProjectDetails projectDetails;
    private final DataSourceConfig dataSourceConfig;
    private Integer measurementUnitID;
    private Integer sourceID;

    // Used to map feature indices to forecast ensembles (used for forecasts)
    private static Map<TimeSeriesIndexKey, Map<Integer, Integer>> indexMapping;

    private Double missingValue;

    @Internal(exclusivePackage = "wres.io")
    public VectorNetCDFValueSaver(String filename,
                                  Future<String> futureHash,
                                  DataSourceConfig dataSourceConfig,
                                  ProjectDetails projectDetails)
    {
        if (filename == null || filename.isEmpty())
        {
            throw new IllegalArgumentException("The passed filename is either null or empty.");
        }
        else if(futureHash == null)
        {
            throw new IllegalArgumentException( "No hash creation operation was passed to the ingestor." );
        }

        this.filePath = Paths.get(filename);
        this.futureHash = futureHash;
        this.projectDetails = projectDetails;
        this.dataSourceConfig = dataSourceConfig;
    }

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
                message += this.filePath.toAbsolutePath().toString();
                message += "' was interupted and could not be completed.";
                LOGGER.error(message);

                throw new IOException( message, e );
            }
            catch ( ExecutionException e )
            {
                String message = "An error occurred while hashing the file '";
                message += this.filePath.toAbsolutePath().toString();
                message += "'.";
                LOGGER.error(message);

                throw new IOException( message, e );
            }
        }
        return this.hash;
    }

    private String getCopyHeader() throws IOException, SQLException
    {
        if (!Strings.hasValue( this.copyHeader ) && this.isForecast())
        {
            this.copyHeader = TimeSeries.getForecastValueParitionName( this.getLead() );
            this.copyHeader += " ";
            this.copyHeader += VectorNetCDFValueSaver.COLUMN_DEFINTIION;
        }
        else if (!Strings.hasValue( this.copyHeader ))
        {
            this.copyHeader = "wres.Observation (" +
                                    "variableposition_id, " +
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
     * @return The id for the source file in the database
     * @throws IOException Thrown if communication with the source file failed
     * @throws SQLException Thrown if an error occurred while accessing the database
     */
    private Integer getSourceID()
            throws IOException, SQLException
    {
        if (this.sourceID == null)
        {
            this.sourceID = DataSources.getSourceID( this.filePath.toAbsolutePath().toString(),
                                                     NetCDF.getInitializedTime( this.getSource() ),
                                                     this.getLead(),
                                                     this.getHash());
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
        this.sourceID = DataSources.getSourceID( this.filePath.toAbsolutePath().toString(),
                                                 NetCDF.getInitializedTime( this.getSource() ),
                                                 this.getLead(),
                                                 this.getHash());

        // Create the script to add the source to the project if it isn't
        // already attached
        String insertProjectSource = "INSERT INTO wres.ProjectSource (project_id, source_id, member)" + NEWLINE;
        insertProjectSource += "SELECT " +
                               this.projectDetails.getId() + ", " +
                               this.getSourceID() + ", " +
                               this.getMember() +
                               NEWLINE;
        insertProjectSource += "WHERE NOT EXISTS (" + NEWLINE;
        insertProjectSource += "    SELECT 1" + NEWLINE;
        insertProjectSource += "    FROM wres.ProjectSource" + NEWLINE;
        insertProjectSource += "    WHERE project_id = " + this.projectDetails.getId() + NEWLINE;
        insertProjectSource += "        AND source_id = " + this.getSourceID() + NEWLINE;
        insertProjectSource += "        AND member = " + this.getMember() + NEWLINE;
        insertProjectSource += ");";

        // If this is a forecast file, we also need to attach the source
        // to the time series it belongs to
        if (this.isForecast())
        {
            // Create the script to add this source file to all time series
            // that it contributes to
            String forecastSourceInsert =
                    "INSERT INTO wres.ForecastSource (forecast_id, source_id)"
                    + NEWLINE;
            forecastSourceInsert +=
                    "SELECT TS.timeseries_id, " + this.sourceID + NEWLINE;
            forecastSourceInsert += "FROM wres.TimeSeries TS" + NEWLINE;
            forecastSourceInsert +=
                    "INNER JOIN wres.VariablePosition VP" + NEWLINE;
            forecastSourceInsert +=
                    "   ON TS.variableposition_id = VP.variableposition_id"
                    + NEWLINE;
            forecastSourceInsert +=
                    "WHERE TS.ensemble_id = " + this.getEnsembleID() + NEWLINE;
            forecastSourceInsert += "   AND TS.initialization_date = '" + NetCDF
                    .getInitializedTime( this.getSource() ) + "'" + NEWLINE;
            forecastSourceInsert += "   AND TS.measurementunit_id = "
                                    + this.getMeasurementUnitID() + NEWLINE;
            forecastSourceInsert +=
                    "   AND VP.variable_id = " + this.getVariableID() + NEWLINE;
            forecastSourceInsert += "   AND NOT EXISTS (" + NEWLINE;
            forecastSourceInsert += "  SELECT 1" + NEWLINE;
            forecastSourceInsert += "  FROM wres.ForecastSource FS" + NEWLINE;
            forecastSourceInsert +=
                    "  WHERE FS.source_id = " + this.sourceID + NEWLINE;
            forecastSourceInsert +=
                    "      AND FS.forecast_id = TS.timeseries_id" + NEWLINE;
            forecastSourceInsert += ");";

            // Attach the source to its time series
            Database.execute( forecastSourceInsert );
        }

        // Attach the source to the project
        Database.execute( insertProjectSource );
    }

    @Override
    protected void execute ()
    {
        try
        {
            // Read the data from the NetCDF file
            this.read();

            // Wait for each statement sent to the database is complete
            while (!this.operations.empty())
            {
                this.operations.pop().get();
            }
        }
        catch (SQLException | IOException | InterruptedException | ExecutionException e)
        {
            LOGGER.error(Strings.getStackTrace(e));
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
                    LOGGER.error("Could not close the NetCDF file: '{}'",
                                 this.filePath.toAbsolutePath().toString());
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }
        }
    }

    /**
     * Add the read value to the queue of values to send to the database
     * @param spatioVariableID The id that ties the value to its locality and
     *                         variable. This will be the timeseries_id for
     *                         forecasts and the variableposition_id for
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
                           .append( DELIMITER )
                           .append( value )
                           .append( NEWLINE );
        }
        else
        {
            // If this is an observation, we want to link the location for the
            // variable to the time that the value is valid, the value itself,
            // the unit it was measured in, and the id of the source file
            this.copyScript.append( spatioVariableID )
                           .append( DELIMITER )
                           .append( NetCDF.getValidTime( this.getSource() ) )
                           .append( DELIMITER )
                           .append(value)
                           .append( DELIMITER )
                           .append(this.getMeasurementUnitID())
                           .append( DELIMITER )
                           .append( this.getSourceID() )
                           .append( NEWLINE );
        }

        // Increase the count of queued values
        this.copyCount++;

        // If the number of queued values reaches the maximum number, save
        // everything
        if (this.copyCount >= SystemSettings.getMaximumCopies())
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
                LOGGER.error(message);
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
                LOGGER.error(message);
                throw new IOException( message );
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
     * @return The member identifier for the data in relation to the left,
     * right, or baseline section of the evaluation equation
     */
    private String getMember()
    {
        String member;

        if (this.projectDetails.getRight().equals( this.dataSourceConfig ))
        {
            member = ProjectDetails.RIGHT_MEMBER;
        }
        else if (this.projectDetails.getLeft().equals( this.dataSourceConfig ))
        {
            member = ProjectDetails.LEFT_MEMBER;
        }
        else
        {
            member = ProjectDetails.BASELINE_MEMBER;
        }

        return member;
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
            this.operations.add(Database.execute( copier ));

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
                this.variableID = Variables.getVariableID(this.getVariable().getShortName(),
                                                          this.getVariable().getUnitsString());
            }
            catch (SQLException e)
            {
                String message = "A variable ID for '" +
                                 this.getVariable().getShortName() +
                                 "' could not be retrieved from the database.";
                LOGGER.error(message);
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
    private void addVariablePositions() throws IOException, SQLException
    {

        // Build a script to add an entry to wres.VariablePosition for each
        // variable and each location in the database that we can tie to a
        // NetCDF index that does not exist
        StringBuilder script = new StringBuilder();

        script.append("INSERT INTO wres.VariablePosition (variable_id, x_position)").append(NEWLINE);
        script.append("SELECT ").append(this.getVariableID()).append(", F.feature_id").append(NEWLINE);
        script.append("FROM wres.Feature F").append(NEWLINE);
        script.append("WHERE F.nwm_index IS NOT NULL").append(NEWLINE);
        script.append("     AND NOT EXISTS (").append(NEWLINE);
        script.append("         SELECT 1").append(NEWLINE);
        script.append("         FROM wres.VariablePosition VP").append(NEWLINE);
        script.append("         WHERE VP.variable_id = ").append(this.getVariableID()).append(NEWLINE);
        script.append("             AND VP.x_position = F.feature_id").append(NEWLINE);
        script.append(");");

        Database.execute(script.toString());
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
        StringBuilder script = new StringBuilder();

        script.append("INSERT INTO wres.TimeSeries (")
              .append("variableposition_id, ")
              .append("ensemble_id, measurementunit_id, ")
              .append("initialization_date)")
              .append(NEWLINE);

        script.append("SELECT VP.variableposition_id, ")
              .append(NEWLINE);

        script.append("    ").append(this.getEnsembleID()).append(", ")
              .append(NEWLINE);

        script.append("    ").append( this.getMeasurementUnitID() ).append(", ")
              .append( NEWLINE);

        script.append("    '")
              .append(NetCDF.getInitializedTime( this.getSource() ))
              .append("'")
              .append(NEWLINE);

        script.append("FROM wres.VariablePosition VP").append(NEWLINE);
        script.append("INNER JOIN wres.Feature F").append(NEWLINE);
        script.append("    ON F.feature_id = VP.x_position").append(NEWLINE);
        script.append("WHERE variable_id = ").append(this.getVariableID()).append(NEWLINE);
        script.append("    AND F.nwm_index IS NOT NULL").append(NEWLINE);
        script.append("    AND NOT EXISTS (").append(NEWLINE);
        script.append("        SELECT 1").append(NEWLINE);
        script.append("        FROM wres.TimeSeries TS").append(NEWLINE);
        script.append("        INNER JOIN wres.ForecastSource FS").append(NEWLINE);
        script.append("            ON FS.forecast_id = TS.timeseries_id").append(NEWLINE);
        script.append("        INNER JOIN wres.ProjectSource PS").append(NEWLINE);
        script.append("            ON PS.source_id = FS.source_id").append(NEWLINE);
        script.append("        WHERE TS.variableposition_id = VP.variableposition_id")
              .append(NEWLINE);

        script.append("            AND PS.project_id = ").append(this.projectDetails.getId())
              .append(NEWLINE);

        script.append("            AND TS.measurementunit_id = ").append(this.getMeasurementUnitID())
              .append( NEWLINE );

        script.append("            AND TS.ensemble_id = " ).append(this.getEnsembleID())
              .append(NEWLINE);

        script.append("            AND TS.initialization_date = '")
              .append(NetCDF.getInitializedTime( this.getSource() ))
              .append("'")
              .append(NEWLINE);

        script.append("    );");

        Database.execute( script.toString() );

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
        return Ensembles.getEnsembleID(NetCDF.getEnsemble(this.getSource()));
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
            initializationTime = NetCDF.getInitializedTime( this.getSource() );
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
        if ( VectorNetCDFValueSaver.indexMapping == null
             || !VectorNetCDFValueSaver.indexMapping.containsKey( key ) )
        {
            // Wait and lock down processing to ensure work isn't duplicated
            synchronized ( VectorNetCDFValueSaver.VARIABLEPOSITION_LOCK )
            {
                // Double check to make sure the necessary work wasn't done
                // while waiting for the lock
                if ( VectorNetCDFValueSaver.indexMapping == null
                     || !VectorNetCDFValueSaver.indexMapping.containsKey( key ) )
                {
                    // Add all missing locations for variables
                    this.addVariablePositions();

                    String keyLabel = "nwm_index";
                    String valueLabel;
                    String script;

                    if ( this.isForecast() )
                    {
                        // Add all time series that correspond with this data
                        this.addTimeSeries();

                        // Form a script that will retrieve the IDs for each
                        // valid location for each time series for this set
                        // of data
                        valueLabel = "timeseries_id";
                        script =
                                "SELECT F.nwm_index, TS.timeseries_id" + NEWLINE
                                +
                                "FROM wres.TimeSeries TS" + NEWLINE +
                                "INNER JOIN wres.VariablePosition VP"
                                + NEWLINE +
                                "     ON VP.variableposition_id = TS.variableposition_id"
                                + NEWLINE +
                                "INNER JOIN wres.ForecastSource FS" +
                                NEWLINE +
                                "       ON FS.forecast_id = TS.timeseries_id"
                                + NEWLINE +
                                "INNER JOIN wres.ProjectSource PS"
                                + NEWLINE +
                                "       ON PS.source_id = FS.source_id"
                                + NEWLINE +
                                "INNER JOIN wres.Feature F" + NEWLINE +
                                "     ON F.feature_id = VP.x_position" + NEWLINE
                                +
                                "WHERE PS.project_id = "
                                + this.projectDetails.getId()
                                + NEWLINE +
                                "   AND PS.member = " + this.getMember()
                                + NEWLINE +
                                "     AND TS.initialization_date = '" +
                                NetCDF.getInitializedTime( this.getSource() ) + "'"
                                + NEWLINE +
                                "     AND F.nwm_index IS NOT NULL;";
                    }
                    else
                    {
                        // Form a script that will map each variable position
                        // to each valid location for the variable in question
                        valueLabel = "variableposition_id";
                        script = "SELECT " + keyLabel + ", " + valueLabel
                                 + NEWLINE +
                                 "FROM wres.VariablePosition VP"
                                 + NEWLINE +
                                 "INNER JOIN wres.Feature F"
                                 + NEWLINE +
                                 "  ON F.feature_id = VP.x_position"
                                 + NEWLINE +
                                 "WHERE F.nwm_index IS NOT null"
                                 + NEWLINE +
                                 "  AND VP.variable_id = "
                                 + this.getVariableID()
                                 + ";";
                    }

                    // If a map hasn't been created yet, create it
                    if ( VectorNetCDFValueSaver.indexMapping == null )
                    {
                        VectorNetCDFValueSaver.indexMapping =
                                new ConcurrentHashMap<>();
                    }

                    // Create a map specific to this set of circumstances
                    Map<Integer, Integer> currentVariableIndices =
                            new ConcurrentHashMap<>();

                    // Populate the newly created map with the generated script
                    Database.populateMap( currentVariableIndices,
                                          script,
                                          keyLabel,
                                          valueLabel );

                    // Add the populated map to the overal collection
                    VectorNetCDFValueSaver.indexMapping.put( key,
                                                             currentVariableIndices );
                }
            }
        }

        // Retrieve the custom tailored index mapping for these circumstances
        return VectorNetCDFValueSaver.indexMapping.get( key );
    }

    /**
     * @return The lead time relative to the initialization date for this data
     * @throws IOException Thrown if communication with the source file failed
     */
    private int getLead() throws IOException
    {
        if (this.lead == null)
        {
            this.lead = NetCDF.getNWMLeadTime(this.getSource());
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
                this.source = NetcdfFile.open(filePath.toAbsolutePath().toString());
            }
            catch (IOException e) {
                LOGGER.error("A file at: '{}' could not be loaded as a NetCDF file.",
                             this.filePath.toAbsolutePath().toString());
                LOGGER.error(Strings.getStackTrace(e));
                throw e;
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

        // Get the mapping between the soon to be read array indices and the
        // IDs for its spatial and variable identifiers
        Map<Integer, Integer> variableIndices = this.getIndexMapping();

        // Ensure that metadata for this file is added linked to the appropriate
        // time series
        this.addSource();

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

                // Scale the read value and send it to be saved
                this.addValuesToSave(positionIndex,
                                     values.getDouble(index) * scaleFactor);
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
