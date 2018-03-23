package wres.io.retrieval;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.VectorOfDoubles;
import wres.io.config.ConfigHelper;
import wres.io.data.caching.MeasurementUnits;
import wres.io.data.caching.UnitConversions;
import wres.io.data.caching.Variables;
import wres.io.data.details.ProjectDetails;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.util.Collections;
import wres.util.ProgressMonitor;
import wres.util.TimeHelper;

abstract class MetricInputIterator implements Iterator<Future<MetricInput<?>>>
{
    protected static final String NEWLINE = System.lineSeparator();

    // Setting the initial window number to -1 ensures that our windows are 0 indexed
    private int windowNumber = -1;
    private Integer windowCount;

    private final Feature feature;

    private final ProjectDetails projectDetails;
    private NavigableMap<LocalDateTime, Double> leftHandMap;
    private VectorOfDoubles climatology;
    private int poolingStep;
    private int finalPoolingStep = 0;

    // This is just for debugging. This is safe for removal and doesn't drive any logic
    // Use Case: "I expect this run to create 15 inputs. It actually generated 612.
    // Something is wrong."
    private int inputCounter;

    private int getWindowNumber()
    {
        return this.windowNumber;
    }

    private void incrementWindowNumber()
    {
        // No incrementing has been done, so we just want to roll with
        // window 0, sequence < 1
        if (this.windowNumber < 0)
        {
            this.windowNumber = 0;
        }
        // If the next sequence is less than the final step, we increment the sequence
        else if ( this.poolingStep + 1 < this.finalPoolingStep )
        {
            this.incrementSequenceStep();
        }
        // Otherwise, we move on to the next window
        else
        {
            this.resetPoolingStep();
            this.windowNumber++;
        }
    }

    private void incrementSequenceStep()
    {
        poolingStep++;
    }

    private void resetPoolingStep()
    {
        this.poolingStep = 0;
    }

    private Integer getWindowCount() throws SQLException, IOException
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

    private void addLeftHandValue(LocalDateTime date, Double measurement)
    {
        if (this.leftHandMap == null)
        {
            this.leftHandMap = new TreeMap<>(  );
        }

        this.leftHandMap.put( date, measurement );
    }

    private VectorOfDoubles getClimatology() throws IOException
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

    MetricInputIterator( final Feature feature, final ProjectDetails projectDetails )
            throws SQLException, IOException
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

        this.finalPoolingStep = this.getFinalPoolingStep();

        ProgressMonitor.setSteps( Long.valueOf( this.getWindowCount() ) );
    }

    protected int getFinalPoolingStep() throws SQLException
    {
        return this.projectDetails.getIssuePoolCount( feature );
    }

    // TODO: Put into its own class
    private void createLeftHandCache() throws SQLException
    {
        Integer desiredMeasurementUnitID =
                MeasurementUnits.getMeasurementUnitID( this.getProjectDetails()
                                                           .getDesiredMeasurementUnit());

        DataSourceConfig left = this.getLeft();
        StringBuilder script = new StringBuilder();
        Integer leftVariableID = Variables.getVariableID( left);

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

        String timeShift = null;

        String variablepositionClause = ConfigHelper.getVariablePositionClause(this.getFeature(), leftVariableID, "");

        if (left.getTimeShift() != null)
        {
            timeShift = "'" + left.getTimeShift().getWidth() + " " + left.getTimeShift().getUnit().value() + "'";
        }

        // TODO: Put this script generation into another class
        script.append("SELECT (O.observation_time");

        if (timeShift != null)
        {
            script.append(" + ").append(timeShift);
        }

        script.append(") AS left_date,").append(NEWLINE);
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

        script.append(";");

        Connection connection = null;
        ResultSet resultSet = null;

        try
        {
            connection = Database.getHighPriorityConnection();
            resultSet = Database.getResults(connection, script.toString());

            while(resultSet.next())
            {
                LocalDateTime date = TimeHelper.convertStringToDate(
                        resultSet.getString( "left_date" ),
                        LocalDateTime::from);
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
                    this.addLeftHandValue( date,
                                           measurement );
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
        boolean next;

        try
        {
            if (ConfigHelper.isForecast( this.getRight() ) &&
                this.getProjectDetails().getPairingMode() != ProjectDetails.PairingMode.TIME_SERIES)
            {
                next = this.finalPoolingStep > 0 && this.poolingStep + 1 < this.finalPoolingStep;

                if (!next)
                {
                    int nextWindowNumber = this.getWindowNumber() + 1;
                    Pair<Integer, Integer> range = this.getProjectDetails().getLeadRange( this.getFeature(), nextWindowNumber );

                    int lastLead = this.getProjectDetails().getLastLead( this.getFeature() );

                    next = range.getLeft() < lastLead &&
                           range.getRight() >= this.getProjectDetails().getMinimumLeadHour()
                           &&
                           range.getRight() <= lastLead;

                    if (!next)
                    {
                        this.getLogger().debug( "There is nothing left to iterate over." );
                    }
                }
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
        catch ( SQLException | IOException e )
        {
            throw new IterationFailedException( "The data provided could not be "
                                                + "used to determine if another "
                                                + "object is present for "
                                                + "iteration.", e );
        }

        if (!next)
        {
            this.getLogger().debug( "We are done iterating." );
        }

        return next;
    }

    @Override
    public Future<MetricInput<?>> next()
    {
        Future<MetricInput<?>> nextInput;

        this.inputCounter++;
        this.incrementWindowNumber();
        try
        {
            nextInput = this.submitForRetrieval();
        }
        catch ( IOException e )
        {
            throw new IterationFailedException( "An exception prevented iteration.", e );
        }

        // Does null have a specific meaning here? Is it expected that
        // null will be returned every time the last call to next() happens?
        // Shouldn't the caller call hasNext() prior to calling next()?
        if (nextInput == null)
        {
            throw new IterationFailedException( "There are no more windows to evaluate" );
        }

        return nextInput;
    }

    /**
     * Creates the object that will retrieve the data in another thread
     * @return A callable object that will create a Metric Input
     * @throws IOException Thrown if a climatology could not be created as needed
     */
    Callable<MetricInput<?>> createRetriever() throws IOException
    {
        // TODO: Pass the leftHandMap instead of the function
        InputRetriever retriever = new InputRetriever(
                this.getProjectDetails(),
                ( firstDate, lastDate ) -> Collections.getValuesInRange( this.leftHandMap, firstDate, lastDate )
        );
        retriever.setFeature(feature);
        retriever.setClimatology( this.getClimatology() );
        retriever.setLeadIteration( this.getWindowNumber() );
        retriever.setIssueDatesPool( this.poolingStep );
        return retriever;
    }

    /**
     * Submits a retrieval object for asynchronous execution
     * @return A MetricInput object that will be fully formed later in the application
     * TODO: document what returning null means so caller can decide what to do
     * when null is returned.
     * @throws IOException Thrown if the retrieval object could not be created
     */
    protected Future<MetricInput<?>> submitForRetrieval() throws IOException
    {
        return Database.submit( this.createRetriever() );
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

    long getFirstLeadInWindow()
            throws SQLException, IOException
    {
        Integer offset = this.getProjectDetails().getLeadOffset( feature );

        if (offset == null)
        {
            throw new NoDataException( "There was not enough data to evaluate a "
                                       + "lead time offset for the location: " +
                                       ConfigHelper.getFeatureDescription( feature ) );
        }
        return this.getProjectDetails().getWindowWidth() + offset;
    }

    abstract int calculateWindowCount() throws SQLException, IOException;

    abstract Logger getLogger();
}
