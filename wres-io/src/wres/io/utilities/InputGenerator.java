package wres.io.utilities;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
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

    public InputGenerator (ProjectConfig projectConfig, Feature feature)
    {
        this.projectConfig = projectConfig;
        this.feature = feature;
    }

    private final ProjectConfig projectConfig;
    private final Feature feature;

    @Override
    public Iterator<Future<MetricInput<?>>> iterator()
    {
        MetricInputIterator iterator = null;
        try {
            iterator =  new MetricInputIterator(this.projectConfig,
                                                this.feature);
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
        // Setting the initial window number to -1 ensures that our windows are 0 indexed
        private int windowNumber = -1;
        private Integer windowCount;
        private Integer variableID;
        private Integer leadOffset;

        private final Feature feature;

        private final ProjectConfig projectConfig;
        private NavigableMap<String, Double> leftHandMap;
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
            NavigableMap<String, Double> values = new TreeMap<>();
            List<Double> futureVector = null;

            String desiredMeasurementUnit = this.projectConfig.getPair().getUnit();
            Integer desiredMeasurementUnitID = MeasurementUnits.getMeasurementUnitID(desiredMeasurementUnit);

            DataSourceConfig left = this.getLeft();
            StringBuilder script = new StringBuilder();
            Integer leftVariableID = ConfigHelper.getVariableID(left);

            String earliestDate = null;
            String latestDate = null;

            Integer timeShift = null;

            if ( projectConfig.getPair()
                              .getDates() != null )
            {
                if ( projectConfig.getPair()
                                  .getDates()
                                  .getEarliest() != null )
                {
                    earliestDate = "'" + projectConfig.getPair()
                                                      .getDates()
                                                      .getEarliest() + "'";
                }

                if ( projectConfig.getPair()
                                  .getDates()
                                  .getLatest() != null )
                {
                    latestDate = "'" + projectConfig.getPair()
                                                    .getDates()
                                                    .getLatest() + "'";
                }
            }

            String variablepositionClause = ConfigHelper.getVariablePositionClause(feature, leftVariableID, "");

            if (left.getTimeShift() != null && left.getTimeShift().getWidth() != 0)
            {
                timeShift = left.getTimeShift().getWidth();
            }

            if (ConfigHelper.isForecast(left))
            {
                List<Integer> forecastIDs = Projects.getProject( this.projectConfig ).getLeftForecastIDs();

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
                script.append("     TS.measurementunit_id,").append(NEWLINE);
                script.append("     (TS.initialization_date + INTERVAL '1 hour' * FV.lead");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(")::text AS left_date").append(NEWLINE);

                script.append("FROM wres.TimeSeries TS").append(NEWLINE);
                script.append("INNER JOIN wres.ForecastValue FV" ).append(NEWLINE);
                script.append("     ON FV.timeseries_id = TS.timeseries_id").append(NEWLINE);
                script.append("WHERE TS.timeseries_id = ")
                      .append(Collections.formAnyStatement(
                              Projects.getProject( projectConfig )
                                      .getLeftForecastIDs(),
                              "int" ))
                      .append(NEWLINE);

                script.append("GROUP BY TS.initialization_date + INTERVAL '1 hour' * FV.lead");

                if (timeShift != null)
                {
                    script.append(" + INTERVAL '1 hour' * ").append(timeShift);
                }

                script.append(", TS.measurementunit_id");
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
                script.append("FROM wres.ProjectSource PS").append(NEWLINE);
                script.append("INNER JOIN wres.Observation O").append(NEWLINE);
                script.append("     ON O.source_id = PS.source_id").append(NEWLINE);
                script.append("WHERE PS.project_id = ")
                      .append(Projects.getProject( this.projectConfig ).getId())
                      .append(NEWLINE);
                script.append("     AND PS.member = 'left'").append(NEWLINE);
                script.append("     AND ").append(variablepositionClause).append(NEWLINE);

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

                if ( projectConfig.getPair()
                                  .getValues() != null )
                {
                    if ( projectConfig.getPair()
                                      .getValues()
                                      .getMinimum() != null )
                    {
                        minimumValue = projectConfig.getPair()
                                                    .getValues()
                                                    .getMinimum();
                    }

                    if ( projectConfig.getPair()
                                      .getValues()
                                      .getMaximum() != null )
                    {
                        maximumValue = projectConfig.getPair()
                                                    .getValues()
                                                    .getMaximum();
                    }
                }

                while(resultSet.next())
                {
                    String date = Database.getValue( resultSet, "left_date" );
                    Double value = Database.getValue( resultSet, "left_value" );

                    int unitID = Database.getValue( resultSet, "measurementunit_id" );

                    if (unitID != desiredMeasurementUnitID)
                    {
                        value = UnitConversions.convert(value, unitID, desiredMeasurementUnit);
                    }

                    if (value == null || ( value >= minimumValue && value <= maximumValue ))
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
                futureVector.removeIf( value -> value == null );
                this.leftHandValues = factory.vectorOf(futureVector.toArray(new Double[futureVector.size()]));
            }
        }

        public String getZeroDate() throws SQLException
        {
            if (this.zeroDate == null)
            {

                DataSourceConfig simulation = this.getSimulation();

                String script = ScriptGenerator.generateZeroDateScript( this.projectConfig,
                                                                        simulation );

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

        public MetricInputIterator(ProjectConfig projectConfig, Feature feature)
                throws SQLException, InvalidPropertiesFormatException,
                NoDataException
        {
            this.projectConfig = projectConfig;
            this.feature = feature;

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

        private Integer getLastLead() throws SQLException
        {
            return Projects.getProject( this.projectConfig )
                           .getLastLead( this.feature );
        }

        private int getLeadOffset() throws NoDataException, SQLException,
                InvalidPropertiesFormatException
        {
            if ( this.leadOffset == null && ConfigHelper.isSimulation( this.getRight() ))
            {
                this.leadOffset = 0;
            }
            else if (this.leadOffset == null)
            {
                this.leadOffset = ConfigHelper.getLeadOffset( this.projectConfig,
                                                              feature);

                if (this.leadOffset == null)
                {
                    String message = "A valid offset for matching lead values could not be determined. ";
                    message += "The first acceptable lead time is ";
                    message += String.valueOf(
                            ConfigHelper.getMinimumLeadHour( this.projectConfig )
                    );

                    if ( ConfigHelper.isMaximumLeadHourSpecified( this.projectConfig ) )
                    {
                        message += ", the last acceptable lead time is ";
                        message += String.valueOf(
                                ConfigHelper.getMaximumLeadHour( this.projectConfig )
                        );
                        message += ",";
                    }

                    message += " and the size of each evaluation window is ";
                    message += String.valueOf(this.projectConfig.getPair().getDesiredTimeAggregation().getPeriod());
                    message += " ";
                    message += String.valueOf(this.projectConfig.getPair().getDesiredTimeAggregation().getUnit());
                    message += "s. ";

                    message += "A full evaluation window could not be found ";
                    message += "between the left hand data and the right ";
                    message += "data that fits within these parameters.";

                    throw new NoDataException( message );
                }
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

        @Override
        public boolean hasNext () {
            boolean next = false;

            try
            {
                if (ConfigHelper.isForecast( this.getRight() ))
                {
                    int nextWindowNumber = this.windowNumber + 1;
                    double beginning = ConfigHelper.getLead (projectConfig, nextWindowNumber) +
                                       this.getLeadOffset();
                    double end = ConfigHelper.getLead(projectConfig, nextWindowNumber + 1) +
                                 this.getLeadOffset();

                    next = beginning < this.getLastLead() &&
                           end >= ConfigHelper.getMinimumLeadHour( projectConfig )
                           &&
                           end <= this.getLastLead();
                }
                else
                {
                    next = this.windowNumber == -1;
                }

                if (!next && this.windowNumber < 0)
                {
                    String message = "Due to the configuration of this project,";
                    message += " there are no valid windows to evaluate. ";
                    message += "The range of all lead times go from {} to ";
                    message += "{}, and the size of the window is {} hours. ";
                    message += "Based on the difference between the initialization ";
                    message += "of the left and right data sets, there is a {} ";
                    message += "hour offset. This puts an initial window out ";
                    message += "range of the specifications.";

                    LOGGER.error(message,
                                  ConfigHelper.getMinimumLeadHour( this.projectConfig ),
                                 this.getLastLead(),
                                 ConfigHelper.getWindowWidth( this.projectConfig ).intValue(),
                                 this.getLeadOffset());
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
                try
                {
                    InputRetriever retriever = new InputRetriever(this.projectConfig,
                                                                  (String firstDate, String lastDate) -> {
                                                                      return Collections.getValuesInRange( this.leftHandMap, firstDate, lastDate );
                                                                  });
                    retriever.setFeature(feature);
                    retriever.setClimatology( this.leftHandValues );
                    retriever.setProgress( this.windowNumber );
                    retriever.setLeadOffset( this.getLeadOffset() );
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
                catch ( NoDataException | SQLException | InvalidPropertiesFormatException e )
                {
                    LOGGER.error( Strings.getStackTrace( e ) );
                    e.printStackTrace();
                }
            }

            return nextInput;
        }
    }
}
