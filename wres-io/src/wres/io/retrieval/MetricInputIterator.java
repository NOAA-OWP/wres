package wres.io.retrieval;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import wres.config.ProjectConfigs;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.io.config.ConfigHelper;
import wres.io.data.details.ProjectDetails;
import wres.io.retrieval.left.LeftHandCache;
import wres.io.utilities.Database;
import wres.io.utilities.NoDataException;
import wres.system.ProgressMonitor;
import wres.util.CalculationException;

abstract class MetricInputIterator implements Iterator<Future<SampleData<?>>>
{
    protected static final String NEWLINE = System.lineSeparator();

    // Setting the initial window number to -1 ensures that our windows are 0 indexed
    private int windowNumber = -1;
    private Integer windowCount;

    private final Feature feature;

    private final ProjectDetails projectDetails;
    protected LeftHandCache leftCache;
    private VectorOfDoubles climatology;
    private int poolingStep;
    private Integer finalPoolingStep;
    private int iterationCount = 0;

    protected int getWindowNumber()
    {
        return this.windowNumber;
    }

    private int getPoolingStep()
    {
        return this.poolingStep;
    }

    private void incrementWindowNumber()
    {
        // If we're using time series metrics, our primary form of iteration is
        // through sequence/pooling steps, not window numbers. We want to
        // ensure that that step always increments. Once it passes the
        // threshold for the final step, the window number will increment,
        // indicating that we have moved on to the next window, which is
        // invalid for metrics using whole time series.
        if ( ProjectConfigs.hasTimeSeriesMetrics(projectDetails.getProjectConfig()))
        {
            this.incrementSequenceStep();

            if (this.getPoolingStep() + 1 > this.finalPoolingStep)
            {
                this.windowNumber = 0;
            }
        }
        // No incrementing has been done, so we just want to roll with
        // window 0, sequence < 1
        else if ( this.windowNumber < 0)
        {
            this.windowNumber = 0;
        }
        // If the next sequence is less than the final step, we increment the sequence
        else if ( this.getPoolingStep() + 1 < this.finalPoolingStep )
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

    private Integer getWindowCount() throws CalculationException
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

    protected VectorOfDoubles getClimatology() throws IOException
    {
        if (this.getProjectDetails().usesProbabilityThresholds() && this.climatology == null)
        {
            ClimatologyBuilder climatologyBuilder = new ClimatologyBuilder( this.getProjectDetails(),
                                                                            this.getProjectDetails().getLeft(),
                                                                            this.getFeature() );
            try
            {
                this.climatology = climatologyBuilder.getClimatology();
            }
            catch ( CalculationException e )
            {
                throw new IOException( "The climatology could not be formed.", e );
            }
        }

        return this.climatology;
    }

    MetricInputIterator( final Feature feature, final ProjectDetails projectDetails )
            throws IOException
    {

        this.projectDetails = projectDetails;

        this.feature = feature;

        try
        {
            this.leftCache = LeftHandCache.getCache( this.projectDetails, this.feature );
        }
        catch ( SQLException e )
        {
            throw new IOException( "Values used as control values for evaluation could not be loaded.", e );
        }

        try
        {
            this.finalPoolingStep = this.getFinalPoolingStep();
        }
        catch ( CalculationException e )
        {
            throw new IOException( "The last pool to be evaluated could not be calculated.", e );
        }

        try
        {
            ProgressMonitor.setSteps( Long.valueOf( this.getWindowCount() ) );
        }
        catch ( CalculationException e )
        {
            throw new IOException( "The number of inputs to evaluate could not be calculated.", e );
        }
    }

    int getFinalPoolingStep() throws CalculationException
    {
        if (this.finalPoolingStep == null)
        {
            this.finalPoolingStep = this.calculateFinalPoolingStep();
        }
        return this.finalPoolingStep;
    }

    protected int calculateFinalPoolingStep() throws CalculationException
    {
        return this.projectDetails.getIssuePoolCount( this.feature );
    }

    @Override
    public boolean hasNext()
    {
        boolean next = false;

        boolean generatesTimeSeriesInputs =
                this.getProjectDetails().getPairingMode() == ProjectDetails.PairingMode.TIME_SERIES;

        boolean isForecast = ConfigHelper.isForecast( this.getRight() );

        try
        {
            // If this is a non-time series metric forecast evaluation, we want
            // to test for iteration through windows based on lead parameters
            if (isForecast && !generatesTimeSeriesInputs)
            {
                next = this.getFinalPoolingStep() > 0 && this.getPoolingStep() + 1 < this.getFinalPoolingStep();

                if (!next)
                {
                    int nextWindowNumber = this.getWindowNumber() + 1;
                    Pair<Integer, Integer> range = this.getProjectDetails().getLeadRange( this.getFeature(), nextWindowNumber );

                    int lastLead = this.getProjectDetails().getLastLead( this.getFeature() );

                    next = range.getLeft() < lastLead &&
                           range.getRight() >= this.getProjectDetails().getMinimumLead() &&
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

            if (!next && this.iterationCount == 0)
            {
                String message = "There was not enough data to evaluate feature: " 
                        + ConfigHelper.getFeatureDescription( this.getFeature() );

                // Flag this to the caller as a NoDataException 
                throw new IterationFailedException( message, new NoDataException( message ) );
            }
        }
        catch ( CalculationException e )
        {
            throw new IterationFailedException( "The data provided could not be "
                                                + "used to calculate if another "
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
    public Future<SampleData<?>> next()
    {
        Future<SampleData<?>> nextInput;

        this.incrementWindowNumber();

        try
        {
            nextInput = this.submitForRetrieval();
        }
        catch ( IOException e )
        {
            throw new IterationFailedException( "An exception prevented iteration.", e );
        }

        this.iterationCount++;
        return nextInput;
    }

    /**
     * Creates the object that will retrieve the data in another thread
     * @return A callable object that will create a Metric Input
     * @throws IOException Thrown if a climatology could not be created as needed
     */
    Callable<SampleData<?>> createRetriever() throws IOException
    {
        InputRetriever retriever = new InputRetriever(
                this.getProjectDetails(),
                this.leftCache::getLeftValues
        );
        retriever.setFeature(feature);
        retriever.setClimatology( this.getClimatology() );
        retriever.setLeadIteration( this.getWindowNumber() );
        retriever.setIssueDatesPool( this.getPoolingStep() );
        return retriever;
    }

    /**
     * Submits a retrieval object for asynchronous execution
     * @return A MetricInput object that will be fully formed later in the application
     * TODO: document what returning null means so caller can decide what to do
     * when null is returned.
     * @throws IOException Thrown if the retrieval object could not be created
     */
    protected Future<SampleData<?>> submitForRetrieval() throws IOException
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
            throws CalculationException
    {
        Integer offset;

        try
        {
            offset = this.getProjectDetails().getLeadOffset( feature );
        }
        catch ( IOException | SQLException e )
        {
            throw new CalculationException("");
        }

        if (offset == null)
        {
            throw new CalculationException( "There was not enough data to evaluate a "
                                       + "lead time offset for the location: " +
                                       ConfigHelper.getFeatureDescription( feature ) );
        }
        return this.getProjectDetails().getWindowWidth() + offset;
    }

    abstract int calculateWindowCount() throws CalculationException;

    abstract Logger getLogger();
}
