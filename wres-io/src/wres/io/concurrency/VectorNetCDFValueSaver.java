package wres.io.concurrency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.io.config.SystemSettings;
import wres.io.data.caching.DataSources;
import wres.io.data.caching.Ensembles;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Variables;
import wres.io.data.details.ForecastDetails;
import wres.io.utilities.Database;
import wres.util.Internal;
import wres.util.NetCDF;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Future;

@Internal(exclusivePackage = "wres.io")
public class VectorNetCDFValueSaver extends WRESRunnable
{
    private static final Object VARIABLEPOSITION_LOCK = new Object();
    private static final Object FORECASTENSEMBLE_LOCK = new Object();

    private final static Logger LOGGER = LoggerFactory.getLogger(VectorNetCDFValueSaver.class);
    private final static String FORECAST_INSERT_SCRIPT = VectorNetCDFValueSaver.createForecastInsertScript();
    private final static String OBSERVATION_INSERT_SCRIPT = createObservationInsertScript();

    private final String variableName;
    private final Path filePath;
    private NetcdfFile source;
    private Integer sourceId;
    private final Stack<Future<?>> operations = new Stack<>();
    List<Object[]> valuesToSave;
    private Long lead;
    private Variable variable;
    private Integer variableID;

    // Used to map feature indices to forecast ensembles (used for forecasts)
    private Map<Integer, Integer> indexForecastMapping;

    // Used to map feature indices to variable positions (used for observations)
    private Map<Integer, Integer> indexPositionMapping;
    private final boolean isForecast;
    int forecastID;

    @Internal(exclusivePackage = "wres.io")
    public VectorNetCDFValueSaver(String filename, String variableName, Boolean isForecast)
    {
        if (filename == null || filename.isEmpty())
        {
            throw new IllegalArgumentException("The passed filename is either null or empty.");
        }
        else if (variableName == null || variableName.isEmpty())
        {
            throw new IllegalArgumentException("The passed in variable name is either null or empty.");
        }

        this.filePath = Paths.get(filename);
        this.variableName = variableName;
        this.isForecast = isForecast;
    }


    private static String createForecastInsertScript()
    {
        StringBuilder script = new StringBuilder();

        script.append("INSERT INTO wres.ForecastValue(forecastensemble_id, lead, forecasted_value").append(NEWLINE);
        script.append("VALUES (?, ?, ?);");

        return script.toString();
    }

    private static String createObservationInsertScript()
    {
        StringBuilder script = new StringBuilder();

        script.append("INSERT INTO wres.Observation(variableposition_id, observation_time, observed_value, measurementunit_id, source_id)").append(NEWLINE);
        script.append("VALUES (?, ?, ?, ?);");

        return script.toString();
    }

    @Override
    protected void execute ()
    {
        try {
            this.addSource();
            this.addVariablePositions();
            this.addForecastEnsembles();
            this.read();
        }
        catch (SQLException e) {
            LOGGER.error(Strings.getStackTrace(e));
        }
        catch (IOException e) {
            LOGGER.error(Strings.getStackTrace(e));
        }
        finally
        {
            if (this.source != null)
            {
                try {
                    this.source.close();
                }
                catch (IOException e) {
                    LOGGER.error("Could not close the NetCDF file: '{}'", this.filePath.toAbsolutePath().toString());
                    LOGGER.error(Strings.getStackTrace(e));
                }
            }
        }
    }

    private void addValuesToSave(Object... values)
    {
        if (valuesToSave == null)
        {
            valuesToSave = new ArrayList<>();
        }

        valuesToSave.add(values);

        if (this.valuesToSave.size() > SystemSettings.maximumDatabaseInsertStatements())
        {
            saveValues();
        }
    }

    private void saveValues()
    {
        if (this.valuesToSave != null && this.valuesToSave.size() > 0) {
            String scriptToSave;

            if (this.isForecast) {
                scriptToSave = FORECAST_INSERT_SCRIPT;
            }
            else {
                scriptToSave = OBSERVATION_INSERT_SCRIPT;
            }

            StatementRunner runner = new StatementRunner(scriptToSave, valuesToSave);
            runner.setOnRun(ProgressMonitor.onThreadStartHandler());
            runner.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            this.operations.add(Database.execute(runner));
            this.valuesToSave = null;
        }
    }

    private void addSource() throws IOException, SQLException {
        try {
            this.sourceId = DataSources.getSourceID(this.filePath.toAbsolutePath().toString(),
                                                    NetCDF.getInitializedTime(this.getSource()),
                                                    this.getLead().intValue());
        }
        catch (SQLException e) {
            LOGGER.error("An error was encountered while retrieving the id for this data source.");
            LOGGER.error(Strings.getStackTrace(e));
            throw e;
        }
    }

    private Variable getVariable() throws IOException {
        if (this.variable == null)
        {
            this.variable = NetCDF.getVariable(this.getSource(), this.variableName);
        }
        return this.variable;
    }

    private Integer getVariableID() throws IOException, SQLException {
        if (this.variableID == null)
        {
            try {
                this.variableID = Variables.getVariableID(this.getVariable().getShortName(), this.getVariable().getUnitsString());
            }
            catch (SQLException e) {
                LOGGER.error("A variable ID for '{}' could not be retrieved from the database.", this.getVariable().getShortName());
                LOGGER.error(Strings.getStackTrace(e));
                throw e;
            }
        }
        return this.variableID;
    }

    private void addVariablePositions() throws IOException, SQLException {
        StringBuilder script = new StringBuilder();

        script.append("INSERT INTO wres.VariablePosition(variable_id, x_position").append(NEWLINE);
        script.append("SELECT ").append(this.getVariableID()).append(", F.feature_id").append(NEWLINE);
        script.append("FROM wres.Feature F").append(NEWLINE);
        script.append("WHERE NOT EXISTS (").append(NEWLINE);
        script.append("     SELECT 1").append(NEWLINE);
        script.append("     FROM wres.VariablePosition VP").append(NEWLINE);
        script.append("     WHERE VP.variable_id = ").append(this.getVariableID()).append(NEWLINE);
        script.append("         AND VP.x_position = F.feature_id").append(NEWLINE);
        script.append(");");

        synchronized (VARIABLEPOSITION_LOCK)
        {
            Database.execute(script.toString());
        }
    }

    private void addForecastEnsembles() throws IOException, SQLException {
        if (this.isForecast)
        {
            StringBuilder script = new StringBuilder();

            script.append("INSERT INTO wres.ForecastEnsemble (forecast_id, variableposition_id, ensemble_id, measurementunit_id)").append(NEWLINE);
            script.append("SELECT ").append(this.getForecastID()).append(", ").append(NEWLINE);
            script.append("     VP.variableposition_id, ").append(NEWLINE);
            script.append("     ").append(this.getEnsembleID()).append(", ").append(NEWLINE);
            script.append("     ").append(MeasurementUnits.getMeasurementUnitID(this.getVariable().getUnitsString())).append(NEWLINE);
            script.append("FROM wres.VariablePositon VP").append(NEWLINE);
            script.append("WHERE variable_id = ").append(this.getVariableID()).append(NEWLINE);
            script.append("     AND NOT EXISTS (").append(NEWLINE);
            script.append("         SELECT 1").append(NEWLINE);
            script.append("         FROM wres.ForecastEnsemble FE").append(NEWLINE);
            script.append("         WHERE FE.forecast_id = ").append(this.getForecastID()).append(NEWLINE);
            script.append("             AND FE.variableposition_id = VP.variableposition_id").append(NEWLINE);
            script.append(");");

            synchronized (FORECASTENSEMBLE_LOCK)
            {
                Database.execute(script.toString());
            }
        }
    }

    private int getEnsembleID() throws SQLException {
        // TODO: Add support for NetCDF Ensemble Data
        return Ensembles.getEnsembleID("Default");
    }

    private Map<Integer, Integer> getIndexForecastMapping() throws IOException, SQLException {
        if (this.indexForecastMapping == null)
        {
            StringBuilder script = new StringBuilder();

            script.append("SELECT VP.x_position, FE.forecastensemble_id").append(NEWLINE);
            script.append("FROM wres.ForecastEnsemble FE").append(NEWLINE);
            script.append("INNER JOIN wres.VariablePosition VP").append(NEWLINE);
            script.append("     ON VP.variableposition_id = FE.variableposition_id").append(NEWLINE);
            script.append("WHERE FE.forecast_id = ").append(this.getForecastID()).append(NEWLINE);
            script.append("     AND VP.variable_id = ").append(this.getVariableID()).append(";");

            this.indexForecastMapping = new TreeMap<>();
            Database.populateMap(this.indexForecastMapping,
                                 script.toString(),
                                 "x_position",
                                 "forecastensemble_id");
        }

        return this.indexForecastMapping;
    }

    private Map<Integer, Integer> getIndexPositionMapping() throws IOException, SQLException {
        if (this.indexPositionMapping == null)
        {
            StringBuilder script = new StringBuilder();

            script.append("SELECT VP.x_position, VP.variableposition_id").append(NEWLINE);
            script.append("FROM wres.VariablePosition VP").append(NEWLINE);
            script.append("WHERE VP.variable_id = ").append(this.getVariableID()).append(";");

            this.indexPositionMapping = new TreeMap<>();

            Database.populateMap(this.indexPositionMapping,
                                 script.toString(),
                                 "x_position",
                                 "variableposition_id");
        }

        return this.indexPositionMapping;
    }

    private Map<Integer, Integer> getIndexMapping() throws IOException, SQLException {
        if (this.isForecast)
        {
            return this.getIndexForecastMapping();
        }
        else
        {
            return this.getIndexPositionMapping();
        }
    }

    private int getForecastID() throws SQLException, IOException {
        if (this.forecastID <= 0)
        {
            ForecastDetails details = new ForecastDetails(this.filePath.toAbsolutePath().toString());
            details.setCreationDate(NetCDF.getInitializedTime(this.getSource()));
            this.forecastID = details.getForecastID();
        }

        return this.forecastID;
    }

    private Long getLead() throws IOException
    {
        if (this.lead == null)
        {
            this.lead = NetCDF.getLeadTime(this.getSource());
        }
        return this.lead;
    }

    private NetcdfFile getSource() throws IOException {
        if (this.source == null)
        {
            try {
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

    private void read() throws IOException, SQLException {
        Variable var = NetCDF.getVariable(this.getSource(), this.variableName);

        Array values = var.read();

        for (int index = 0; index < values.getSize(); ++index)
        {
            if (this.getIndexMapping().containsKey(index))
            {
                Integer positionIndex = this.getIndexMapping().get(index);
                if (this.isForecast)
                {
                    this.addValuesToSave(positionIndex,
                                         this.lead,
                                         values.getObject(index));
                }
                else
                {
                    this.addValuesToSave(positionIndex,
                                         NetCDF.getValidTime(this.getSource()),
                                         values.getObject(index),
                                         MeasurementUnits.getMeasurementUnitID(var.getUnitsString()),
                                         this.sourceId);
                }
            }
        }

        saveValues();
    }

    @Override
    protected String getTaskName () {
        return "VectorNetCDFValueSaver:" + this.filePath.toString();
    }

    @Override
    protected Logger getLogger () {
        return this.LOGGER;
    }
}
