package wres.io.utilities;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.Conditions;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeAggregationConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricInput;
import wres.datamodel.VectorOfDoubles;
import wres.io.concurrency.InputRetriever;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.Scenarios;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.UnitConversions;
import wres.io.grouping.LabeledScript;
import wres.util.NotImplementedException;
import wres.util.ProgressMonitor;
import wres.util.Strings;

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
        catch (SQLException | NotImplementedException | InvalidPropertiesFormatException e) {
            LOGGER.error("A MetricInputIterator could not be created.");
            LOGGER.error(Strings.getStackTrace(e));
        }
        return iterator;
    }

    private static final class MetricInputIterator implements Iterator<Future<MetricInput<?>>>
    {
        private int windowNumber;
        private Integer windowCount;
        private Integer variableID;
        private Integer lastLead;

        private final Conditions.Feature feature;
        private final ProjectConfig projectConfig;
        private Map<String, Double> leftHandMap;
        private VectorOfDoubles leftHandValues;
        private String zeroDate;

        private DataSourceConfig getLeft()
        {
            return this.projectConfig.getInputs().getLeft();
        }

        private DataSourceConfig getRight()
        {
            return this.projectConfig.getInputs().getRight();
        }

        private DataSourceConfig getBaseline()
        {
            DataSourceConfig baseline = null;

            if (ConfigHelper.hasBaseline( this.projectConfig ))
            {
                baseline = this.projectConfig.getInputs().getBaseline();
            }

            return baseline;
        }

        private void createLeftHandCache() throws SQLException
        {
            Map<String, Double> values = new HashMap<>();
            List<Double> futureVector = null;

            String desiredMeasurementUnit = this.projectConfig.getPair().getUnit();
            Integer desiredMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(desiredMeasurementUnit);
            DataSourceConfig left = this.getLeft();

            StringBuilder script = new StringBuilder();
            Integer leftVariableID = ConfigHelper.getVariableID(left);

            String earliestDate = null;
            String latestDate = null;

            Integer timeShift = null;

            if (projectConfig.getConditions().getDates() != null)
            {
                if (projectConfig.getConditions().getDates().getEarliest() != null)
                {
                    earliestDate = "'" + projectConfig.getConditions().getDates().getEarliest() + "'";
                }

                if (projectConfig.getConditions().getDates().getLatest() != null)
                {
                    latestDate = "'" + projectConfig.getConditions().getDates().getLatest() + "'";
                }
            }

            String variablepositionClause = ConfigHelper.getVariablePositionClause(this.feature, leftVariableID);

            if (left.getTimeShift() != null && left.getTimeShift().getWidth() != 0)
            {
                timeShift = left.getTimeShift().getWidth();
            }

            if (ConfigHelper.isForecast(left))
            {
                // TODO: This will be a mess if we don't have the ability to select "Assim data" rather than all
                script.append("SELECT ");
                if (left.getTimeAggregation() != null) {
                    script.append(left.getTimeAggregation().getFunction());
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

                if (!left.getScenario().equalsIgnoreCase( "variable" ))
                {
                    script.append("     AND F.scenario_id = ")
                          .append( Scenarios.getScenarioID( left.getScenario(),
                                                            left.getType().value()))
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
                script.append("     AND O.scenario_id = ")
                      .append(Scenarios.getScenarioID( left.getScenario(),
                                                       left.getType().value() ));

                if (earliestDate != null)
                {
                    script.append("     AND O.observation_time");

                    if (timeShift != null)
                    {
                        script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                    }

                    script.append(" >= ").append(earliestDate).append(NEWLINE);
                }

                if (latestDate != null)
                {
                    script.append("     AND O.observation_time");

                    if (timeShift != null)
                    {
                        script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                    }

                    script.append(" <= ").append(latestDate).append(NEWLINE);
                }
            }

            script.append(";");

            Connection connection = null;
            ResultSet resultSet = null;

            try
            {
                connection = Database.getHighPriorityConnection();
                resultSet = Database.getResults(connection, script.toString());

                Double minimumValue = -Double.MAX_VALUE;
                Double maximumValue = Double.MAX_VALUE;

                if (projectConfig.getConditions().getValues() != null)
                {
                    if (projectConfig.getConditions().getValues().getMinimum() != null)
                    {
                        minimumValue = projectConfig.getConditions().getValues().getMinimum();
                    }

                    if (projectConfig.getConditions().getValues().getMaximum() != null)
                    {
                        maximumValue = projectConfig.getConditions().getValues().getMaximum();
                    }
                }

                while(resultSet.next())
                {
                    String date = resultSet.getString("left_date");
                    Double value = resultSet.getDouble("left_value");
                    int unitID = resultSet.getInt("measurementunit_id");

                    if (unitID != desiredMeasurementUnitID)
                    {
                        value = UnitConversions.convert(value, unitID, desiredMeasurementUnit);
                    }

                    if (value >= minimumValue && value <= maximumValue)
                    {
                        values.put( date, value );
                    }

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


            if (futureVector != null)
            {
                DataFactory factory = DefaultDataFactory.getInstance();
                this.leftHandValues = factory.vectorOf(futureVector.toArray(new Double[futureVector.size()]));
            }
        }

        public String getZeroDate() throws SQLException
        {
            if (this.zeroDate == null)
            {

                DataSourceConfig simulation = this.getSimulation();

                String script = ScriptGenerator.generateZeroDateScript( this.projectConfig,
                                                                        simulation,
                                                                        this.feature);

                this.zeroDate = Database.getResult(script, "zero_date");
            }

            return this.zeroDate;
        }

        private DataSourceConfig getSimulation()
        {
            DataSourceConfig simulation = null;

            if (!ConfigHelper.isForecast( this.getRight()))
            {
                simulation = this.getRight();
            }
            else if (!ConfigHelper.isForecast( this.getBaseline() ))
            {
                simulation = this.getBaseline();
            }

            return simulation;
        }

        public MetricInputIterator(ProjectConfig projectConfig, Conditions.Feature feature)
                throws SQLException, InvalidPropertiesFormatException
        {
            this.projectConfig = projectConfig;
            this.feature = feature;
            this.createLeftHandCache();

            // TODO: This needs a better home
            ProgressMonitor.setSteps( Long.valueOf( this.getWindowCount() ) );
        }

        private Integer getWindowCount()
                throws SQLException, InvalidPropertiesFormatException
        {
            if ( this.windowCount == null && ConfigHelper.isForecast( this.getRight() ))
            {

                // TODO: Add validation; throw error if start >= last
                double start = this.projectConfig.getConditions().getFirstLead();
                Integer last = this.getLastLead();

                if (last == null)
                {
                    throw new IllegalArgumentException( "The final lead time for the data set for: " +
                                                        this.getRight()
                                                            .getVariable()
                                                            .getValue() +
                                                        " could not be determined.");
                }

                TimeAggregationConfig timeAggregationConfig =
                        ConfigHelper.getTimeAggregation( this.getRight() );

                double windowWidth = ConfigHelper.getWindowWidth( timeAggregationConfig );
                double windowSpan = this.getLastLead() - start;

                this.windowCount =
                        ((Double)Math.ceil( windowSpan / windowWidth)).intValue();
            }
            else if (this.windowCount == null)
            {
                this.windowCount = 1;
            }

            return this.windowCount;
        }

        private Integer getLastLead()
                throws SQLException, InvalidPropertiesFormatException
        {
            if (this.lastLead == null)
            {
                int scenarioID = Scenarios.getScenarioID( this.getRight().getScenario(),
                                                          this.getRight().getType().value() );
                LabeledScript lastLeadScript = ScriptGenerator.generateFindLastLead(this.getVariableID(),
                                                                                    this.feature,
                                                                                    scenarioID,
                                                                                    ConfigHelper.isForecast( this.getRight() ));

                lastLead = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());

                if (this.lastLead == null)
                {
                    throw new IllegalStateException( "No data could be found to generate pairs for." );
                }
                if (projectConfig.getConditions().getLastLead() < lastLead)
                {
                    lastLead = projectConfig.getConditions().getLastLead();
                }
            }
            return this.lastLead;
        }

        /**
         * @return the id for the variable in the database
         * @throws SQLException
         */
        private Integer getVariableID() throws SQLException
        {
            if (this.variableID == null)
            {
                this.variableID = ConfigHelper.getVariableID(this.getRight());
            }
            return this.variableID;
        }

        @Override
        public boolean hasNext () {
            boolean next = false;
            try
            {

                if (ConfigHelper.isForecast( this.getRight() ))
                {
                    Double windowWidth;
                    TimeAggregationConfig timeAggregationConfig =
                            ConfigHelper.getTimeAggregation( this.getRight() );
                    windowWidth =
                            ConfigHelper.getWindowWidth( timeAggregationConfig );

                    double beginning = windowWidth * this.windowNumber;
                    double end = windowWidth * (this.windowNumber + 1);

                    next = beginning < this.getLastLead() &&
                           beginning < this.projectConfig.getConditions().getLastLead() &&
                           end >= this.projectConfig.getConditions().getFirstLead();
                }
                else
                {
                    next = this.windowNumber == 0;
                }
            }
            catch ( SQLException | InvalidPropertiesFormatException e )
            {
                LOGGER.error(Strings.getStackTrace( e ));
            }
            return next;
        }

        @Override
        public Future<MetricInput<?>> next () {
            Future<MetricInput<?>> nextInput = null;

            if (this.hasNext())
            {
                this.windowNumber++;
                InputRetriever retriever = new InputRetriever(this.projectConfig,
                                                              this.feature,
                                                              this.windowNumber,
                                                              (String date) -> {
                                                                    return this.leftHandMap.getOrDefault(date,null);
                                                                    },
                                                              this.leftHandValues);

                if (this.getSimulation() != null)
                {
                    try
                    {
                        retriever.setZeroDate( this.getZeroDate() );
                    }
                    catch ( SQLException e )
                    {
                        e.printStackTrace();
                    }
                }

                nextInput = Database.submit(retriever);
            }

            return nextInput;
        }
    }
}
