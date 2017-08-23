package wres.io.utilities;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.ForecastRange;
import wres.config.generated.ProjectConfig;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.MetricInput;
import wres.io.concurrency.InputRetriever;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.ForecastTypes;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.UnitConversions;
import wres.io.grouping.LabeledScript;
import wres.util.NotImplementedException;
import wres.util.ProgressMonitor;
import wres.util.Strings;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Future;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class InputGenerator implements Iterable<Future<MetricInput<?>>> {

    private static final String NEWLINE = System.lineSeparator();
    private static final Logger LOGGER = LoggerFactory.getLogger(InputGenerator.class);
    /**
     * Constructor
     * @param projectConfig The project configuration that will guide input creation
     * @param feature The geographic feature configuration that describes the area for the data to retrieve
     */
    public InputGenerator (ProjectConfig projectConfig, Conditions.Feature feature)
    {
        this.projectConfig = projectConfig;
        this.feature = feature;
    }

    private final ProjectConfig projectConfig;
    private final Conditions.Feature feature;

    @Override
    public Iterator<Future<MetricInput<?>>> iterator()
    {
        MetricInputIterator iterator = null;
        try {
            iterator =  new MetricInputIterator(this.projectConfig, this.feature);
        }
        catch (SQLException | NotImplementedException e) {
            LOGGER.error("A MetricInputIterator could not be created.");
            LOGGER.error(Strings.getStackTrace(e));
        }

        return iterator;
    }

    private static final class MetricInputIterator implements Iterator<Future<MetricInput<?>>>
    {
        private int windowNumber;
        private Integer lastWindowNumber;
        private Integer variableID;

        private final Conditions.Feature feature;
        private final ProjectConfig projectConfig;
        private Map<String, Double> leftHandMap;
        private VectorOfDoubles leftHandValues;

        private void createLeftHandCache() throws SQLException
        {
            Map<String, Double> values = new HashMap<>();
            List<Double> futureVector = null;

            String desiredMeasurementUnit = this.projectConfig.getPair().getUnit();
            Integer desiredMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(desiredMeasurementUnit);
            DataSourceConfig left = this.projectConfig.getInputs().getLeft();

            StringBuilder script = new StringBuilder();
            Integer leftVariableID = ConfigHelper.getVariableID(left);

            Integer timeShift = null;

            String variablepositionClause = ConfigHelper.getVariablePositionClause(this.feature, leftVariableID);

            if (left.getTimeShift() != null && left.getTimeShift().getWidth() != 0)
            {
                timeShift = left.getTimeShift().getWidth();
            }

            if (ConfigHelper.isForecast(left))
            {
                // TODO: This will be a mess if we don't have the ability to select "Assim data" rather than all
                script.append("SELECT ");
                if (left.getRollingTimeAggregation() != null) {
                    script.append(left.getRollingTimeAggregation().getFunction());
                }
                else
                {
                    // Default is the average - will not change if there is only one value
                    script.append("AVG");
                }
                script.append("(FV.forecasted_value) AS left_value,").append(NEWLINE);
                script.append("     FE.measurementunit_id,").append(NEWLINE);
                script.append("     (F.forecast_date + INTERVAL '1 hour' * FV.lead");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(")::text AS left_date").append(NEWLINE);

                script.append("FROM wres.Forecast F").append(NEWLINE);
                script.append("INNER JOIN wres.ForecastEnsemble FE").append(NEWLINE);
                script.append("     ON FE.forecast_id = F.forecast_id").append(NEWLINE);
                script.append("INNER JOIN wres.ForecastValue FV" ).append(NEWLINE);
                script.append("     ON FV.forecastensemble_id = FE.forecastensemble_id").append(NEWLINE);
                script.append("WHERE ").append(variablepositionClause).append(NEWLINE);

                if (left.getRange() != ForecastRange.VARIABLE)
                {
                    script.append("     AND F.forecasttype_id = ")
                          .append(ForecastTypes.getForecastTypeId(left.getRange().value()))
                          .append(NEWLINE);
                }

                script.append("GROUP BY F.forecast_date + INTERVAL '1 hour' * FV.lead");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(", FE.measurementunit_id");
            }
            else
            {
                script.append("SELECT (O.observation_time");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(")::text AS left_date,").append(NEWLINE);
                script.append("     O.observed_value AS left_value,").append(NEWLINE);
                script.append("     O.measurementunit_id").append(NEWLINE);
                script.append("FROM wres.Observation O").append(NEWLINE);
                script.append("WHERE ").append(variablepositionClause).append(NEWLINE);
            }

            script.append(";");

            Connection connection = null;
            ResultSet resultSet = null;

            try
            {
                connection = Database.getHighPriorityConnection();
                resultSet = Database.getResults(connection, script.toString());

                while(resultSet.next())
                {
                    String date = resultSet.getString("left_date");
                    Double value = resultSet.getDouble("left_value");
                    int unitID = resultSet.getInt("measurementunit_id");

                    if (unitID != desiredMeasurementUnitID)
                    {
                        value = UnitConversions.convert(value, unitID, desiredMeasurementUnit);
                    }

                    values.put(date, value);

                    if (ConfigHelper.usesProbabilityThresholds(projectConfig))
                    {
                        if (futureVector == null)
                        {
                            futureVector = new ArrayList<>();
                        }
                        futureVector.add(value);
                    }
                }
            }
            finally
            {
                if (resultSet != null)
                {
                    resultSet.close();
                }

                if (connection != null)
                {
                    Database.returnHighPriorityConnection(connection);
                }
            }
            this.leftHandMap = values;

            DataFactory factory = DefaultDataFactory.getInstance();

            if (ConfigHelper.usesProbabilityThresholds(projectConfig))
            {
                this.leftHandValues = factory.vectorOf(futureVector.toArray(new Double[futureVector.size()]));
            }
        }

        public MetricInputIterator(ProjectConfig projectConfig, Conditions.Feature feature) throws SQLException
        {
            this.projectConfig = projectConfig;
            this.feature = feature;
            this.createLeftHandCache();
        }

        private Integer getLastWindowNumber() throws SQLException
        {
            if (this.lastWindowNumber == null)
            {
                LabeledScript lastLeadScript = ScriptGenerator.generateFindLastLead(this.getVariableID(),
                                                                                    this.feature,
                                                                                    projectConfig.getInputs().getLeft().getRange());
                this.lastWindowNumber = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());

                // If the last window number could not be determined from the database, set it to a number that should
                // always yield false on validity checks
                if (this.lastWindowNumber == null)
                {
                    this.lastWindowNumber = -1;
                }
            }
            return this.lastWindowNumber;
        }

        /**
         * @return the id for the variable in the database
         * @throws SQLException
         */
        private Integer getVariableID() throws SQLException
        {
            if (this.variableID == null)
            {
                this.variableID = ConfigHelper.getVariableID(projectConfig.getInputs().getRight());
            }
            return this.variableID;
        }

        @Override
        public boolean hasNext () {
            boolean isNext = false;

            try
            {
                isNext = ConfigHelper.leadIsValid(projectConfig, this.windowNumber + 1, this.getLastWindowNumber());
            }
            catch (SQLException error)
            {
                LOGGER.error("The last window for this project could not be calculated.");
                LOGGER.error(Strings.getStackTrace(error));
            }

            return isNext;
        }

        @Override
        public Future<MetricInput<?>> next () {
            Future<MetricInput<?>> nextInput = null;

            if (ConfigHelper.leadIsValid(this.projectConfig, this.windowNumber + 1, this.lastWindowNumber))
            {
                this.windowNumber++;
                InputRetriever retriever = new InputRetriever(this.projectConfig,
                                                              this.feature,
                                                              this.windowNumber,
                                                              (String date) -> {
                                                                    return this.leftHandMap.getOrDefault(date,null);
                                                                    },
                                                              this.leftHandValues);
                retriever.setOnRun(ProgressMonitor.onThreadStartHandler());
                retriever.setOnComplete(ProgressMonitor.onThreadCompleteHandler());
                nextInput = Database.submit(retriever);
            }

            return nextInput;
        }
    }
}
