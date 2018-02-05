package wres.io.retrieval;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.Future;

import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
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

    protected int getWindowNumber()
    {
        return this.windowNumber;
    }

    protected void incrementWindowNumber()
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
            this.setPoolingStep( 0 );
            this.windowNumber++;
        }
    }

    protected void incrementSequenceStep()
    {
        poolingStep++;
    }

    protected void setPoolingStep( int poolingStep )
    {
        this.poolingStep = poolingStep;
    }

    protected Integer getWindowCount() throws SQLException, IOException
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

    protected void addLeftHandValue(LocalDateTime date, Double measurement)
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

    public MetricInputIterator( final Feature feature,
                                final ProjectDetails projectDetails )
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

        this.finalPoolingStep = this.projectDetails.getPoolCount( feature );

        // TODO: This needs a better home
        // x2; 1 step for retrieval, 1 step for calculation
        ProgressMonitor.setSteps( Long.valueOf( this.getWindowCount() ) * 2 );
    }

    // TODO: Put into its own class
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
        boolean next = false;

        try
        {
            if (ConfigHelper.isForecast( this.getRight() ))
            {
                next = this.finalPoolingStep > 0 && this.poolingStep + 1 < this.finalPoolingStep;

                if (!next)
                {
                    int nextWindowNumber = this.getWindowNumber() + 1;
                    int offset = this.getProjectDetails().getLeadOffset( this.getFeature() );

                    // Creates a range of (beginning, end], or
                    // (next Window Number * lead frequency + Lead Offset, next Window Number * lead frequency + lead offset + lead period]
                    Integer end = this.getProjectDetails().getLeadPeriod() +
                                  nextWindowNumber * this.getProjectDetails().getLeadFrequency() +
                                  offset;
                    Integer beginning = nextWindowNumber * this.getProjectDetails().getLeadFrequency() + offset;

                    int lastLead = this.getProjectDetails().getLastLead( this.getFeature() );

                    next = beginning < lastLead &&
                           end >= this.getProjectDetails().getMinimumLeadHour()
                           &&
                           end <= lastLead;

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
            this.getLogger().error( Strings.getStackTrace( e ));
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
        Future<MetricInput<?>> nextInput = null;

        this.inputCounter++;
        this.incrementWindowNumber();
        try
        {
            // TODO: Pass the leftHandMap instead of the function
            InputRetriever retriever = new InputRetriever( this.getProjectDetails(),
                                                           ( LocalDateTime firstDate, LocalDateTime lastDate) -> Collections
                                                                   .getValuesInRange( this.leftHandMap, firstDate, lastDate ) );
            retriever.setFeature(feature);
            retriever.setClimatology( this.getClimatology() );
            retriever.setProgress( this.getWindowNumber() );
            retriever.setPoolingStep( this.poolingStep );
            retriever.setOnRun( ProgressMonitor.onThreadStartHandler() );
            retriever.setOnComplete( ProgressMonitor.onThreadCompleteHandler() );

            nextInput = Database.submit(retriever);
        }
        catch ( IOException e )
        {
            throw new NoSuchElementException( Strings.getStackTrace( e ) );
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
            throws SQLException, IOException
    {
        Integer offset = this.getProjectDetails().getLeadOffset( feature );

        if (offset == null)
        {
            throw new NoDataException( "There was not enough data to evaluate a "
                                       + "lead time offset for the location: " +
                                       ConfigHelper.getFeatureDescription( feature ) );
        }
        return ( this.getWindowNumber() * this.getProjectDetails().getWindowWidth()) +
               offset;
    }

    abstract int calculateWindowCount() throws SQLException, IOException;

    abstract Logger getLogger();
}
