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

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricInput;
import wres.datamodel.VectorOfDoubles;
import wres.io.concurrency.InputRetriever;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.Projects;
import wres.io.data.caching.UnitConversions;
import wres.io.grouping.LabeledScript;
import wres.util.Collections;
import wres.util.NotImplementedException;
import wres.util.ProgressMonitor;
import wres.util.Strings;

/**
 * Interprets a project configuration and spawns asynchronous metric input retrieval operations
 */
public class InputGenerator implements Iterable<Future<MetricInput<?>>> {

    private static final String NEWLINE = System.lineSeparator();
    private static final Logger LOGGER = LoggerFactory.getLogger(InputGenerator.class);

    public InputGenerator (ProjectConfig projectConfig, Feature leftFeature, Feature rightFeature, Feature baselineFeature)
    {
        this.projectConfig = projectConfig;
        this.leftFeature = leftFeature;
        this.rightFeature = rightFeature;
        this.baselineFeature = baselineFeature;
    }

    private final ProjectConfig projectConfig;
    private final Feature leftFeature;
    private final Feature rightFeature;
    private final Feature baselineFeature;

    @Override
    public Iterator<Future<MetricInput<?>>> iterator()
    {
        MetricInputIterator iterator = null;
        try {
            iterator =  new MetricInputIterator(this.projectConfig,
                                                this.leftFeature,
                                                this.rightFeature,
                                                this.baselineFeature);
        }
        catch (SQLException | NotImplementedException | InvalidPropertiesFormatException e) {
            LOGGER.error("A MetricInputIterator could not be created.");
            LOGGER.error(Strings.getStackTrace(e));
        }
        catch ( NoDataException e )
        {
            LOGGER.error("A MetricInputIterator could not be created. " + ""
                         + "There's no data to iterate over.");
            LOGGER.error(Strings.getStackTrace( e ));
        }
        return iterator;
    }

    private static final class MetricInputIterator implements Iterator<Future<MetricInput<?>>>
    {
        private int windowNumber;
        private Integer windowCount;
        private Integer variableID;
        private Integer lastLead;
        private Integer leadOffset;

        private final Feature leftFeature;
        private final Feature rightFeature;
        private final Feature baselineFeature;

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

        private void createLeftHandCache() throws SQLException, NoDataException
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

            String variablepositionClause = ConfigHelper.getVariablePositionClause(leftFeature, leftVariableID, "");

            if (left.getTimeShift() != null && left.getTimeShift().getWidth() != 0)
            {
                timeShift = left.getTimeShift().getWidth();
            }

            if (ConfigHelper.isForecast(left))
            {
                List<Integer> forecastIDs = Projects.getProject( this.projectConfig.getName() ).getLeftForecastIDs();

                if (forecastIDs.size() == 0)
                {
                    throw new NoDataException("There is no forecast data that " +
                                              "can be loaded for comparison " +
                                              "purposes. Please supply new " +
                                              "data or adjust the project " +
                                              "specifications.");
                }

                // TODO: This will be a mess if we don't have the ability to select "Assim data" rather than all
                script.append("SELECT ");
                if (left.getExistingTimeAggregation() != null) {
                    script.append(left.getExistingTimeAggregation().getFunction());
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
                script.append("     AND F.forecast_id = ")
                      .append(Collections.formAnyStatement(
                              Projects.getProject( projectConfig.getName() )
                                      .getLeftForecastIDs(),
                              "int" ))
                      .append(NEWLINE);

                script.append("GROUP BY F.forecast_date + INTERVAL '1 hour' * FV.lead");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(", FE.measurementunit_id");
            }
            else
            {
                List<Integer> leftSources = Projects.getProject( this.projectConfig.getName() ).getLeftSources();

                if (leftSources.size() == 0)
                {
                    throw new NoDataException( "There are no observation data "
                                               + "pair with. Please ensure "
                                               + "data exists that can be "
                                               + "ingested and that the project "
                                               + "is properly configured." );
                }

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
                script.append("     AND O.source_id = ")
                      .append( Collections.formAnyStatement( Projects.getProject( this.projectConfig )
                                                                     .getLeftSources(), "int" ))
                      .append(NEWLINE);

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

            if (values.size() == 0)
            {
                LOGGER.error( "Statement for left hand data retrieval:" );
                LOGGER.error( script.toString() );
                throw new NoDataException( "No data for the left hand side of the evaluation could be loaded. Please check your specifications." );

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
                                                                        simulation);

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

        public MetricInputIterator(ProjectConfig projectConfig, Feature leftFeature, Feature rightFeature, Feature baselineFeature)
                throws SQLException, InvalidPropertiesFormatException,
                NoDataException
        {
            this.projectConfig = projectConfig;
            this.leftFeature = leftFeature;
            this.rightFeature = rightFeature;
            this.baselineFeature = baselineFeature;

            this.createLeftHandCache();

            // TODO: This needs a better home
            // x2; 1 step for retrieval, 1 step for calculation
            ProgressMonitor.setSteps( Long.valueOf( this.getWindowCount() ) * 2 );
        }

        private Integer getWindowCount()
                throws SQLException, InvalidPropertiesFormatException,
                NoDataException
        {
            if ( this.windowCount == null && ConfigHelper.isForecast( this.getRight() ))
            {

                // TODO: Add validation; throw error if start >= last
                int start = this.getFirstLeadInWindow();
                Integer last = this.getLastLead();

                if (last == null)
                {
                    throw new IllegalArgumentException( "The final lead time for the data set for: " +
                                                        this.getRight()
                                                            .getVariable()
                                                            .getValue() +
                                                        " could not be determined.");
                }

                double windowWidth = ConfigHelper.getWindowWidth( this.projectConfig );
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
                throws SQLException, InvalidPropertiesFormatException,
                NoDataException
        {
            if (this.lastLead == null)
            {
                int projectID = Projects.getProjectID( this.projectConfig.getName());
                LabeledScript lastLeadScript = ScriptGenerator.generateFindLastLead(this.getVariableID(),
                                                                                    this.rightFeature,
                                                                                    projectID,
                                                                                    ConfigHelper.isForecast( this.getRight() ));

                lastLead = Database.getResult(lastLeadScript.getScript(), lastLeadScript.getLabel());

                if (this.lastLead == null)
                {
                    throw new NoDataException( "No data could be found to generate pairs for." );
                }
                if (projectConfig.getConditions().getLastLead() < lastLead)
                {
                    lastLead = projectConfig.getConditions().getLastLead();
                }
            }
            return this.lastLead;
        }

        private int getLeadOffset() throws NoDataException, SQLException,
                InvalidPropertiesFormatException
        {
            if (this.leadOffset == null)
            {
                this.leadOffset = ConfigHelper.getLeadOffset( this.projectConfig,
                                                              leftFeature,
                                                              rightFeature);
            }

            return this.leadOffset;
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

        private int getFirstLeadInWindow()
                throws InvalidPropertiesFormatException, NoDataException,
                SQLException
        {
            return (this.windowNumber * ConfigHelper.getWindowWidth( this.projectConfig ).intValue()) +
                   this.getLeadOffset();
        }

        private int getLastLeadInWindow()
                throws InvalidPropertiesFormatException, NoDataException,
                SQLException
        {
            return ((this.windowNumber + 1) * ConfigHelper.getWindowWidth( this.projectConfig ).intValue()) +
                   this.getLeadOffset();
        }

        @Override
        public boolean hasNext () {
            boolean next = false;
            try
            {

                if (ConfigHelper.isForecast( this.getRight() ))
                {
                    double beginning = this.getFirstLeadInWindow();
                    double end = this.getLastLeadInWindow();

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
            catch ( NoDataException e )
            {
                LOGGER.error("The last lead time for pairing could not be " +
                             "determined; There is no data to pair and " +
                             "iterate over.");
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
                                                              (String date) -> {
                                                                    return this.leftHandMap.getOrDefault(date,null);
                                                                    });
                retriever.setRightFeature( this.rightFeature );
                retriever.setBaselineFeature( this.baselineFeature );
                retriever.setClimatology( this.leftHandValues );
                retriever.setProgress( this.windowNumber );
                retriever.setOnRun( ProgressMonitor.onThreadStartHandler() );
                retriever.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

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
