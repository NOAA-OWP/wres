package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.io.config.ConfigHelper;
import wres.io.config.OrderedSampleMetadata;
import wres.io.project.Project;
import wres.io.retrieval.left.LeftHandCache;
import wres.io.utilities.Database;
import wres.system.ProgressMonitor;
import wres.util.CalculationException;
import wres.util.IterationFailedException;
import wres.util.TimeHelper;

/**
 * Creates and launches thread tasks used to create {@link wres.datamodel.sampledata.SampleData} on demand.
 * Used to throttle the creation and execution of {@link wres.datamodel.sampledata.SampleData} retrieval operations
 */
abstract class SampleDataIterator implements Iterator<Future<SampleData<?>>>
{
    /**
     * Shortcut term used to create a new line in generated text
     */
    protected static final String NEWLINE = System.lineSeparator();

    /**
     * The feature whose {@link wres.datamodel.sampledata.SampleData} will be generated for
     */
    private final Feature feature;

    /**
     * The collection of relevant data used to determine how to form a series of threads used to load
     * {@link wres.datamodel.sampledata.SampleData} collections
     */
    private final Project project;

    /**
     * Object used to determine what left handed data to load for evaluations
     */
    LeftHandCache leftCache;

    /**
     * Lazy generated collection of values to use for metrics requiring a set of climatological values
     */
    private VectorOfDoubles climatology;

    /**
     * A numeric id used to identify when the last collection of values over issue pool will occur
     * TODO: Detemine if this should be moved to {@link wres.io.retrieval.PoolingSampleDataIterator}
     */
    private Integer finalPoolingStep;

    /**
     * Determines where pairing output for the threads generating {@link wres.datamodel.sampledata.SampleData}
     * should be created
     */
    private Path outputPathForPairs;

    /**
     * A queue containing the parameters used to create each task that will generate sets of
     * {@link wres.datamodel.sampledata.SampleData}
     */
    private final Queue<OrderedSampleMetadata> sampleMetadata = new LinkedList<>(  );

    /**
     * Adds parameters to the sampleMetadata queue to later iterate over
     * @param orderedSampleMetadata Parameters describing how to form a single instance of
     * {@link wres.datamodel.sampledata.SampleData}
     */
    void addSample( final OrderedSampleMetadata orderedSampleMetadata)
    {
        this.sampleMetadata.add( orderedSampleMetadata );
        this.getLogger().debug(
                "Evaluating {}: {}",
                ConfigHelper.getFeatureDescription( this.getFeature() ),
                orderedSampleMetadata
        );
    }

    /**
     * @return The name of {@link wres.datamodel.sampledata.SampleData} instances to generate
     */
    int getSampleCount()
    {
        return this.sampleMetadata.size();
    }

    /**
     * @return The feature to generate {@link wres.datamodel.sampledata.SampleData} for
     */
    protected Feature getFeature()
    {
        return this.feature;
    }

    /**
     * @return The project that determines how to generate {@link wres.datamodel.sampledata.SampleData} retrieval tasks
     */
    protected Project getProject()
    {
        return this.project;
    }

    /**
     * Lazy getter for climatological metric data
     * <p>Only generates data for probability thresholding</p>
     * @return A collection of climatological values to use for probability threshold metrics
     * @throws IOException Thrown if the climatology could not be generated.
     */
    VectorOfDoubles getClimatology() throws IOException
    {
        // We only want to generate the climatology if we need one
        if ( this.getProject().usesProbabilityThresholds() && this.climatology == null)
        {
            ClimatologyBuilder climatologyBuilder = new ClimatologyBuilder( this.getProject(),
                                                                            this.getProject().getLeft(),
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

    /**
     * The constructor
     * @param feature The feature to generate {@link wres.datamodel.sampledata.SampleData} for
     * @param project The project that determines how to generate {@link wres.datamodel.sampledata.SampleData}
     * @param outputPathForPairs The path to where to store paired {@link wres.datamodel.sampledata.SampleData} values
     * {@link wres.datamodel.sampledata.SampleData} generation task
     * @throws IOException
     */
    SampleDataIterator( Feature feature,
                        Project project,
                        Path outputPathForPairs)
            throws IOException
    {
        if (this.getLogger().isTraceEnabled())
        {
            this.getLogger().trace( "Iterator created for {}", ConfigHelper.getFeatureDescription( feature ) );
        }

        this.project = project;
        this.feature = feature;
        this.outputPathForPairs = outputPathForPairs;

        try
        {
            if (this.getLogger().isTraceEnabled())
            {
                this.getLogger().trace( "Gathering left hand data...");
            }

            this.leftCache = LeftHandCache.getCache( this.project, this.feature );

            if (this.getLogger().isTraceEnabled())
            {
                this.getLogger().trace( "{}: Left hand data gathered.", LocalDateTime.now() );
            }
        }
        catch ( SQLException e )
        {
            throw new IOException( "Values used as control values for evaluation could not be loaded.", e );
        }

        try
        {
            if (this.getLogger().isTraceEnabled())
            {
                this.getLogger().trace( "Calculating the final pooling step.");
            }

            this.finalPoolingStep = this.getFinalPoolingStep();

            if (this.getLogger().isTraceEnabled())
            {
                this.getLogger().trace( "The final pooling step has been calculated.");
            }
        }
        catch ( CalculationException e )
        {
            throw new IOException( "The last pool to be evaluated could not be calculated.", e );
        }

        try
        {
            this.calculateSamples();
        }
        catch ( CalculationException e )
        {
            throw new IOException( "The time windows to evaluate could not be calculated.", e );
        }

        ProgressMonitor.setSteps( (long)this.getSampleCount() );
    }

    /**
     * Generates a list of TimeWindows that will generate SampleData objects
     * 
     * @throws CalculationException if the samples could not be calculated
     */
    protected abstract void calculateSamples() throws CalculationException;

    /**
     * Get the left and right lead bounds for a sample
     * @param sampleNumber The number of the sample, 0 indexed, that indicates the order of evaluation
     * @return A pair of durations describing the left and right lead bounds for the sample
     * @throws CalculationException Thrown if the frequency, period, or offset for lead bounds could not be calculated
     */
    Pair<Duration, Duration> getLeadBounds(final int sampleNumber) throws CalculationException
    {
        Duration beginning;
        Duration end;

        Duration offset;
        try
        {
            offset = Duration.of( this.getProject().getLeadOffset( this.getFeature() ).longValue(),
                                  TimeHelper.LEAD_RESOLUTION );
        }
        catch ( IOException | SQLException e )
        {
            throw new CalculationException( "The offset between observed values and "
                                            + "forecasted values are needed to determine "
                                            + "when a lead range should begin, but could "
                                            + "not be loaded.",
                                            e );
        }
        
        Duration leadFrequency = this.getProject().getLeadFrequency();
        Duration leadPeriod = this.getProject().getLeadPeriod();

        // If the lead offset is positive, forecasts at this offset value
        // need to be captured in the first lead bounds, so start at the offset 
        // minus the lead period and iterate forwards from there in multiples of
        // lead frequency, otherwise iterate forwards from the zero lower-bound. 
        // However, skip this interval if it is smaller than the desired time scale
        // See #60307
        if ( !offset.isZero() && !offset.minus( this.getProject().getDesiredTimeScale().getPeriod() ).isNegative() )
        {
            beginning = offset.minus( leadPeriod ).plus( leadFrequency.multipliedBy( sampleNumber ) );
        }
        else
        {
            beginning = offset.plus( leadFrequency.multipliedBy( sampleNumber ) );
        }
        
        end = beginning.plus( leadPeriod ); 

        return Pair.of(beginning, end);
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
        return this.project.getIssuePoolCount( this.feature );
    }

    @Override
    public boolean hasNext()
    {
        return !this.sampleMetadata.isEmpty();
    }

    @Override
    public Future<SampleData<?>> next()
    {
        if (this.sampleMetadata.isEmpty())
        {
            throw new NoSuchElementException( "There are no more sample data to evaluate for " +
                                              ConfigHelper.getFeatureDescription( this.getFeature() ) );
        }
        Future<SampleData<?>> nextInput;

        try
        {
            OrderedSampleMetadata metadataToSubmit = this.sampleMetadata.remove();

            if (this.getLogger().isTraceEnabled())
            {
                this.getLogger().trace( "Issuing task for {}", metadataToSubmit );
            }

            nextInput = this.submitForRetrieval(metadataToSubmit);

        }
        catch ( IOException e )
        {
            throw new IterationFailedException( "An exception prevented iteration.", e );
        }

        return nextInput;
    }

    Path getOutputPathForPairs()
    {
        return this.outputPathForPairs;
    }

    /**
     * Creates the object that will retrieve the data in another thread
     * @return A callable object that will create a Metric Input
     * @throws IOException Thrown if a climatology could not be created as needed
     */
    Callable<SampleData<?>> createRetriever(final OrderedSampleMetadata sampleMetadata) throws IOException
    {
        SampleDataRetriever retriever = new SampleDataRetriever(
                sampleMetadata,
                this.leftCache::getLeftValues,
                this.outputPathForPairs
        );
        retriever.setClimatology( this.getClimatology() );

        return retriever;
    }

    /**
     * Submits a retrieval object for asynchronous execution
     * @param sampleMetadata the sample metadata
     * @return A MetricInput object that will be fully formed later in the application
     * TODO: document what returning null means so caller can decide what to do
     * when null is returned.
     * @throws IOException Thrown if the retrieval object could not be created
     */
    protected Future<SampleData<?>> submitForRetrieval(final OrderedSampleMetadata sampleMetadata) throws IOException
    {
        return Database.submit( this.createRetriever(sampleMetadata) );
    }

    protected DataSourceConfig getLeft()
    {
        return this.getProject().getLeft();
    }

    protected DataSourceConfig getRight()
    {
        return this.getProject().getRight();
    }

    protected DataSourceConfig getBaseline()
    {
        return this.getProject().getBaseline();
    }

    abstract Logger getLogger();
}
