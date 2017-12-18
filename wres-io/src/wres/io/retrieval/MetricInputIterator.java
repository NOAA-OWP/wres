package wres.io.retrieval;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.InvalidPropertiesFormatException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.ProjectConfig;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.VectorOfDoubles;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.UnitConversions;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.util.Collections;
import wres.util.ProgressMonitor;
import wres.util.Strings;

abstract class MetricInputIterator implements Iterator<Future<MetricInput<?>>>
{
    protected static final String NEWLINE = System.lineSeparator();

    // Setting the initial window number to -1 ensures that our windows are 0 indexed
    private int windowNumber = -1;
    private Integer windowCount;

    private final Feature feature;

    private final ProjectDetails projectDetails;
    private NavigableMap<String, Double> leftHandMap;
    private VectorOfDoubles climatology;
    private int sequenceStep;
    private int finalSequenceStep = 0;

    protected int getWindowNumber()
    {
        return this.windowNumber;
    }

    protected void incrementWindowNumber()
    {
        if (this.sequenceStep <= this.finalSequenceStep)
        {
            this.incrementSequenceStep();
        }
        else
        {
            this.setSequenceStep( 0 );
            this.windowNumber++;
        }
    }

    protected void incrementSequenceStep()
    {
        sequenceStep++;
    }

    protected void setSequenceStep(int sequenceStep)
    {
        this.sequenceStep = sequenceStep;
    }

    protected void setFinalSequenceStep(int finalSequenceStep)
    {
        this.finalSequenceStep = finalSequenceStep;
    }

    protected Integer getWindowCount() throws NoDataException, SQLException,
            InvalidPropertiesFormatException
    {
        if (this.windowCount == null)
        {
            this.windowCount = this.calculateWindowCount();
        }

        return this.windowCount;
    }

    protected Feature getFeature()
    {
        return this.feature;
    }

    protected ProjectDetails getProjectDetails()
    {
        return this.projectDetails;
    }

    protected void addLeftHandValue(String date, Double measurement)
    {
        if (this.leftHandMap == null)
        {
            this.leftHandMap = new TreeMap<>(  );
        }

        this.leftHandMap.put( date, measurement );
    }

    protected VectorOfDoubles getClimatology() throws IOException
    {
        if (this.getProjectDetails().usesProbabilityThresholds() && this.climatology == null)
        {
            ClimatologyBuilder climatologyBuilder = new ClimatologyBuilder( this.getProjectDetails(),
                                                                            this.getProjectDetails().getLeft(),
                                                                            this.getFeature() );
            this.climatology = climatologyBuilder.getClimatology();
        }

        return this.climatology;
    }

    public MetricInputIterator( final ProjectConfig projectConfig,
                                final Feature feature,
                                final ProjectDetails projectDetails )
            throws SQLException, NoDataException,
            InvalidPropertiesFormatException
    {

        this.projectDetails = projectDetails;

        this.feature = feature;

        this.createLeftHandCache();

        if (this.leftHandMap == null || this.leftHandMap.size() == 0)
        {
            throw new NoDataException( "No data for the left hand side of " +
                                       " the evaluation could be loaded. " +
                                       "Please check your specifications." );
        }

        this.finalSequenceStep = this.projectDetails.getRollingWindowCount( feature );

        // TODO: This needs a better home
        // x2; 1 step for retrieval, 1 step for calculation
        ProgressMonitor.setSteps( Long.valueOf( this.getWindowCount() ) * 2 );
    }

    void createLeftHandCache() throws SQLException, NoDataException
    {
        Integer desiredMeasurementUnitID =
                MeasurementUnits.getMeasurementUnitID( this.getProjectDetails()
                                                           .getDesiredMeasurementUnit());

        DataSourceConfig left = this.getLeft();
        StringBuilder script = new StringBuilder();
        Integer leftVariableID = ConfigHelper.getVariableID(left);

        String earliestDate = this.getProjectDetails().getEarliestDate();
        String latestDate = this.getProjectDetails().getLatestDate();

        if (earliestDate != null)
        {
            earliestDate = "'" + earliestDate + "'";
        }

        if (latestDate != null)
        {
            latestDate = "'" + latestDate + "'";
        }

        Integer timeShift = null;

        String variablepositionClause = ConfigHelper.getVariablePositionClause(this.getFeature(), leftVariableID, "");

        if (left.getTimeShift() != null && left.getTimeShift().getWidth() != 0)
        {
            timeShift = left.getTimeShift().getWidth();
        }

        if (ConfigHelper.isForecast(left))
        {
            List<Integer> forecastIDs = this.getProjectDetails().getLeftForecastIDs();

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
                  .append( Collections.formAnyStatement(
                          this.getProjectDetails().getLeftForecastIDs(),
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
                  .append(this.getProjectDetails().getId())
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

            while(resultSet.next())
            {
                String date = Database.getValue( resultSet, "left_date" );
                Double measurement = Database.getValue( resultSet, "left_value" );

                int unitID = Database.getValue( resultSet, "measurementunit_id" );

                if ( unitID != desiredMeasurementUnitID
                     && measurement != null )
                {
                    measurement = UnitConversions.convert( measurement,
                                                           unitID,
                                                           this.getProjectDetails()
                                                               .getDesiredMeasurementUnit());
                }

                if (measurement == null ||
                    ( measurement >= this.getProjectDetails().getMinimumValue() &&
                      measurement <= this.getProjectDetails().getMaximumValue() ))
                {
                    this.addLeftHandValue( date, measurement );
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
    }

    @Override
    public boolean hasNext()
    {
        boolean next = false;

        try
        {
            if (ConfigHelper.isForecast( this.getRight() ))
            {
                int nextWindowNumber = this.getWindowNumber() + 1;
                int beginning = this.getProjectDetails().getLead(nextWindowNumber) +
                                   this.getProjectDetails().getLeadOffset( this.getFeature() );
                int end = this.getProjectDetails().getLead(nextWindowNumber + 1) +
                             this.getProjectDetails().getLeadOffset( this.getFeature() );

                next = beginning < this.getProjectDetails().getLastLead( this.getFeature() ) &&
                       end >= this.getProjectDetails().getMinimumLeadHour() &&
                       end <= this.getProjectDetails().getLastLead( this.getFeature() );
            }
            else
            {
                next = this.getWindowNumber() == -1;
            }

            if (!next && this.getWindowNumber() < 0)
            {
                String message = "Due to the configuration of this project,";
                message += " there are no valid windows to evaluate. ";
                message += "The range of all lead times go from {} to ";
                message += "{}, and the size of the window is {} hours. ";
                message += "Based on the difference between the initialization ";
                message += "of the left and right data sets, there is a {} ";
                message += "hour offset. This puts an initial window out ";
                message += "range of the specifications.";

                this.getLogger().error(message,
                                       this.getProjectDetails().getMinimumLeadHour(),
                                       this.getProjectDetails().getLastLead( feature ),
                                       this.getProjectDetails().getWindowWidth(),
                                       this.getProjectDetails().getLeadOffset( this.getFeature() ));
            }
        }
        catch ( SQLException | InvalidPropertiesFormatException e )
        {
            this.getLogger().error( Strings.getStackTrace( e ));
        }
        catch ( NoDataException e )
        {
            this.getLogger().error("The last lead time for pairing could not be " +
                                   "determined; There is no data to pair and " +
                                   "iterate over.");
        }

        return next;
    }

    @Override
    public Future<MetricInput<?>> next()
    {
        Future<MetricInput<?>> nextInput = null;

        if (this.hasNext())
        {
            this.incrementWindowNumber();
            try
            {
                // TODO: Pass the leftHandMap instead of the function
                InputRetriever retriever = new InputRetriever( this.getProjectDetails(),
                                                               (String firstDate, String lastDate) -> {
                                                                   return wres.util.Collections
                                                                           .getValuesInRange( this.leftHandMap, firstDate, lastDate );
                                                               } );
                retriever.setFeature(feature);
                retriever.setClimatology( this.getClimatology() );
                retriever.setProgress( this.getWindowNumber() );
                retriever.setLeadOffset( this.getProjectDetails()
                                             .getLeadOffset( this.getFeature() ) );
                retriever.setSequenceStep( this.sequenceStep );
                retriever.setOnRun( ProgressMonitor.onThreadStartHandler() );
                retriever.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

                nextInput = Database.submit(retriever);
            }
            catch ( SQLException | IOException e )
            {
                this.getLogger().error( Strings.getStackTrace( e ) );
            }
        }

        if (nextInput == null)
        {
            throw new NoSuchElementException( "There are no more windows to evaluate" );
        }

        return nextInput;
    }

    protected DataSourceConfig getLeft()
    {
        return this.getProjectDetails().getLeft();
    }

    protected DataSourceConfig getRight()
    {
        return this.getProjectDetails().getRight();
    }

    protected DataSourceConfig getBaseline()
    {
        return this.getProjectDetails().getBaseline();
    }

    protected int getFirstLeadInWindow()
            throws InvalidPropertiesFormatException, NoDataException,
            SQLException
    {
        return ( this.getWindowNumber() * this.getProjectDetails().getWindowWidth()) +
               this.getProjectDetails().getLeadOffset( this.getFeature() );
    }

    abstract int calculateWindowCount()
            throws SQLException,
            InvalidPropertiesFormatException,
            NoDataException;

    abstract Logger getLogger();
}
