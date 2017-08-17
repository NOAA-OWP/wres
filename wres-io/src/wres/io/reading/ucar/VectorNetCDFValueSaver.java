package wres.io.reading.ucar;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ucar.ma2.Array;
import ucar.nc2.NetcdfFile;
import ucar.nc2.Variable;
import wres.io.concurrency.StatementRunner;
import wres.io.concurrency.WRESRunnable;
import wres.io.config.SystemSettings;
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
class VectorNetCDFValueSaver extends WRESRunnable
{
    private static final Object VARIABLEPOSITION_LOCK = new Object();
    private static final Object FORECASTENSEMBLE_LOCK = new Object();

    private final static Logger LOGGER = LoggerFactory.getLogger(VectorNetCDFValueSaver.class);
    private String forecastInsertScript;

    private final String variableName;
    private final Path filePath;
    private NetcdfFile source;
    private final Stack<Future<?>> operations = new Stack<>();
    List<Object[]> valuesToSave;
    private Integer lead;
    private Variable variable;
    private Integer variableID;
    private String variablePositionPartitionName;
    private String forecastValuePartitionName;

    // Used to map feature indices to forecast ensembles (used for forecasts)
    private Map<Integer, Integer> indexMapping;

    private Double missingValue;
    int forecastID;

    @Internal(exclusivePackage = "wres.io")
    public VectorNetCDFValueSaver(String filename, String variableName)
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
    }


    private String getForecastInsertScript() throws IOException, SQLException {
        if (this.forecastInsertScript == null) {
            StringBuilder script = new StringBuilder();

            script.append("INSERT INTO ").
                    append(ForecastDetails.getForecastValueParitionName(this.getLead()))
                  .append("(forecastensemble_id, lead, forecasted_value)")
                  .append(NEWLINE);
            script.append("VALUES (?, ?, ?);");

            this.forecastInsertScript = script.toString();
        }
        return this.forecastInsertScript;
    }

    @Override
    protected void execute ()
    {
        try {
            if (NetCDF.getNWMRange(this.getSource()).equalsIgnoreCase("analysis") && this.getLead() < 3)
            {
                LOGGER.warn("Skipping ingestion of the file '{}'; prepatory analysis data should not be used for evaluation.",
                            this.filePath.toString());
                return;
            }

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

    private void addValuesToSave(Integer forecastEnsembleID, Double value) throws IOException, SQLException {
        if (!measurementIsValid(value))
        {
            return;
        }

        if (valuesToSave == null)
        {
            valuesToSave = new ArrayList<>();
        }

        valuesToSave.add(new Object[]{forecastEnsembleID, this.getLead(), value});

        if (this.valuesToSave.size() > SystemSettings.maximumDatabaseInsertStatements())
        {
            saveValues();
        }
    }

    private Double getMissingValue()
    {
        if (this.missingValue == null)
        {
            try {
                this.missingValue = NetCDF.getMissingValue(this.getVariable());
            }
            catch (IOException e) {
                LOGGER.error("The variable could not be retrieved from the NetCDF source file.");
            }
        }

        if (this.missingValue == null)
        {
            try {
                this.missingValue = NetCDF.getGlobalMissingValue(this.getSource());
            }
            catch (IOException e) {
                LOGGER.error("The source NetCDF file could not be loaded.");
            }
        }

        if (this.missingValue == null)
        {
            this.missingValue = -99999999.99;
        }

        return this.missingValue;
    }

    private boolean measurementIsValid(Double measurement)
    {
        return this.getMissingValue() == null || !String.valueOf(measurement)
                                                   .equalsIgnoreCase(String.valueOf(this.getMissingValue()));
    }

    private void saveValues() throws IOException, SQLException
    {
        if (this.valuesToSave != null && this.valuesToSave.size() > 0)
        {
            StatementRunner runner = new StatementRunner(this.getForecastInsertScript(), valuesToSave);
            runner.setOnRun(ProgressMonitor.onThreadStartHandler());
            runner.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
            this.operations.add(Database.execute(runner));
            this.valuesToSave = null;
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
                this.variableID = Variables.getVariableID(this.getVariable().getShortName(),
                                                          this.getVariable().getUnitsString());
            }
            catch (SQLException e) {
                LOGGER.error("A variable ID for '{}' could not be retrieved from the database.",
                             this.getVariable().getShortName());
                LOGGER.error(Strings.getStackTrace(e));
                throw e;
            }
        }
        return this.variableID;
    }

    private String getVariablePositionPartitionName () throws IOException, SQLException {
        if (this.variablePositionPartitionName == null)
        {
            this.variablePositionPartitionName = Variables.getVariablePositionPartitionName(this.getVariableID());
        }
        return this.variablePositionPartitionName;
    }

    private void addVariablePositions() throws IOException, SQLException {
        StringBuilder script = new StringBuilder();

        script.append("INSERT INTO ").append(this.getVariablePositionPartitionName()).append("(variable_id, x_position)").append(NEWLINE);
        script.append("SELECT ").append(this.getVariableID()).append(", F.feature_id").append(NEWLINE);
        script.append("FROM wres.Feature F").append(NEWLINE);
        script.append("WHERE NOT EXISTS (").append(NEWLINE);
        script.append("     SELECT 1").append(NEWLINE);
        script.append("     FROM ").append(this.getVariablePositionPartitionName()).append(" VP").append(NEWLINE);
        script.append("     WHERE VP.variable_id = ").append(this.getVariableID()).append(NEWLINE);
        script.append("         AND VP.x_position = F.feature_id").append(NEWLINE);
        script.append(");");

        synchronized (VARIABLEPOSITION_LOCK)
        {
            Database.execute(script.toString());
        }
    }

    private void addForecastEnsembles() throws IOException, SQLException
    {
        StringBuilder script = new StringBuilder();

        script.append("INSERT INTO wres.ForecastEnsemble (forecast_id, variableposition_id, ensemble_id, measurementunit_id)").append(NEWLINE);
        script.append("SELECT ").append(this.getForecastID()).append(", ").append(NEWLINE);
        script.append("     VP.variableposition_id, ").append(NEWLINE);
        script.append("     ").append(this.getEnsembleID()).append(", ").append(NEWLINE);
        script.append("     ").append(MeasurementUnits.getMeasurementUnitID(this.getVariable().getUnitsString())).append(NEWLINE);
        script.append("FROM ").append(this.getVariablePositionPartitionName()).append(" VP").append(NEWLINE);
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

    private int getEnsembleID() throws SQLException, IOException
    {
        return Ensembles.getEnsembleID(NetCDF.getEnsemble(this.getSource()));
    }

    private Map<Integer, Integer> getIndexMapping() throws IOException, SQLException {
        if (this.indexMapping == null)
        {
            StringBuilder script = new StringBuilder();

            script.append("SELECT VP.x_position, FE.forecastensemble_id").append(NEWLINE);
            script.append("FROM wres.ForecastEnsemble FE").append(NEWLINE);
            script.append("INNER JOIN ").append(this.getVariablePositionPartitionName()).append(" VP").append(NEWLINE);
            script.append("     ON VP.variableposition_id = FE.variableposition_id").append(NEWLINE);
            script.append("WHERE FE.forecast_id = ").append(this.getForecastID()).append(NEWLINE);
            script.append("     AND VP.variable_id = ").append(this.getVariableID()).append(";");

            this.indexMapping = new TreeMap<>();
            Database.populateMap(this.indexMapping,
                                 script.toString(),
                                 "x_position",
                                 "forecastensemble_id");
        }

        return this.indexMapping;
    }

    private int getForecastID() throws SQLException, IOException {
        if (this.forecastID <= 0)
        {
            ForecastDetails details = new ForecastDetails(this.filePath.toAbsolutePath().toString());
            details.setCreationDate(NetCDF.getInitializedTime(this.getSource()));
            details.setLead(this.getLead());
            details.setRange(NetCDF.getNWMRange(this.getSource()));
            this.forecastID = details.getForecastID();
        }

        return this.forecastID;
    }

    private int getLead() throws IOException
    {
        if (this.lead == null)
        {
            this.lead = NetCDF.getNWMLeadTime(this.getSource());
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
        double scaleFactor = NetCDF.getScaleFactor(var);

        Array values = var.read();

        for (int index = 0; index < values.getSize(); ++index)
        {
            if (this.getIndexMapping().containsKey(index))
            {
                Integer positionIndex = this.getIndexMapping().get(index);
                this.addValuesToSave(positionIndex, values.getDouble(index) * scaleFactor);
            }
        }

        saveValues();
    }

    @Override
    protected Logger getLogger() {
        return this.LOGGER;
    }
}
