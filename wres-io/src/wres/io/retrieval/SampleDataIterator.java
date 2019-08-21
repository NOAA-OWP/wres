package wres.io.retrieval;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.Feature;
import wres.config.generated.IntBoundsType;
import wres.config.generated.PairConfig;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.time.TimeWindow;
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
    private static final Logger LOGGER = LoggerFactory.getLogger( SampleDataIterator.class );

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
     * @throws IOException Thrown if left handed data could not be loaded
     * @throws IOException Thrown if the limit for pooling could not be calculated
     * @throws IOException Thrown if the sample data to iterate over could not be calculated
     */
    SampleDataIterator( Feature feature, Project project, Path outputPathForPairs) throws IOException
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
    protected abstract void calculateSamples();

    /**
     * <p>Get the left and right lead bounds for a sample
     * 
     * <p>TODO: replace with #56213. Cannot be confident that this is working correctly under
     * all circumstances, particularly for the default system behavior, which is to generate
     * one (data dependent) lead duration pool for each lead duration present in the pairs.
     * 
     * @param sampleNumber The number of the sample, 0 indexed, that indicates the order of evaluation
     * @return A pair of durations describing the left and right lead bounds for the sample
     * @throws CalculationException Thrown if the frequency, period, or offset for lead bounds 
     *            could not be calculated
     */
    Pair<Duration, Duration> getLeadBounds( final int sampleNumber )
    {
        Pair<Duration, Duration> leadBounds = null;
        
        // Are leadTimesPoolingWindows defined?
        // If so, they must be respected and not modified by the data: #63407
        PairConfig pairConfig = this.getProject().getProjectConfig().getPair();
        
        if ( Objects.nonNull( pairConfig.getLeadTimesPoolingWindow() ) )
        {
            leadBounds = this.getLeadBoundsForLeadTimesPoolingWindows( this.getProject().getProjectConfig(),
                                                                       sampleNumber );
        }
        // No leadTimesPoolingWindows
        else
        {
            // Unbounded
            Duration lowerBound = TimeWindow.DURATION_MIN;
            Duration upperBound = TimeWindow.DURATION_MAX;
            
            if ( Objects.nonNull( pairConfig.getLeadHours() ) )
            {
                IntBoundsType bounds = pairConfig.getLeadHours();
                
                if( Objects.nonNull( bounds.getMinimum() ) )
                {
                    lowerBound = Duration.of( pairConfig.getLeadHours().getMinimum(), ChronoUnit.HOURS );
                }
                
                if( Objects.nonNull( bounds.getMaximum() ) )
                {
                    upperBound = Duration.of( pairConfig.getLeadHours().getMaximum(), ChronoUnit.HOURS );
                }
            }
            
            leadBounds = Pair.of( lowerBound, upperBound );
        }

        return this.getAdjustedLeadBounds( leadBounds );
    }        

    /**
     * <p>Returns the lower and upper bounds of the next lead duration pooling window
     * whose sample number corresponds to the input. There is no validation that
     * the sampleNumber exceeds the declared upper bound for all windows.
     * 
     * <p>TODO: replace with #56213
     * 
     * @param project The project declaration
     * @param sampleNumber The number of the sample, 0 indexed, that indicates the order of evaluation
     * @return A pair of durations describing the left and right lead bounds for the sample number
     * @throws CalculationException if the iteration exceeds the declared upper bound
     * @throws NullPointerException if the project is null
     */

    private Pair<Duration, Duration> getLeadBoundsForLeadTimesPoolingWindows( ProjectConfig project,
                                                                              int sampleNumber )
    {
        Objects.requireNonNull( project );

        // Get the pair declaration, which includes pooling windows
        PairConfig pairConfig = project.getPair();

        IntBoundsType leadHours = pairConfig.getLeadHours();

        PoolingWindowConfig leadTimesPoolingWindow = pairConfig.getLeadTimesPoolingWindow();

        // Create the elements necessary to increment the windows
        ChronoUnit periodUnits = ChronoUnit.valueOf( leadTimesPoolingWindow.getUnit()
                                                                           .toString()
                                                                           .toUpperCase() );
        // Period associated with the leadTimesPoolingWindow
        Duration periodOfLeadTimesPoolingWindow = Duration.of( leadTimesPoolingWindow.getPeriod(), periodUnits );

        // Exclusive lower bound
        Duration earliestLeadDurationExclusive = Duration.ofHours( leadHours.getMinimum() );

        // Duration by which to increment. Defaults to the period associated
        // with the leadTimesPoolingWindow, otherwise the frequency.
        Duration increment = periodOfLeadTimesPoolingWindow;
        if ( Objects.nonNull( leadTimesPoolingWindow.getFrequency() ) )
        {
            increment = Duration.of( leadTimesPoolingWindow.getFrequency(), periodUnits );
        }

        // Determine the increment for the current sample number
        Duration incrementMultipliedBySampleNumber = increment.multipliedBy( sampleNumber );

        // Add the increment for the current sample number to the lower bound
        Duration earliestExclusive = earliestLeadDurationExclusive.plus( incrementMultipliedBySampleNumber );

        // Upper bound of the current window, which is the lower bound plus the period      
        Duration latestInclusive = earliestExclusive.plus( periodOfLeadTimesPoolingWindow );

        // TODO: it would be preferable to allow a bounds-check here, but some 
        // implementations of SampleDataIterator::calculateSamples currently call 
        // this method to iterate beyond the bounds and then check. 
        // All this will disappear with #56213, so leaving for now.
        
        // Validate the upper bound as being less than or equal to the overall upper bound
//        Duration latestLeadDurationInclusive = Duration.ofHours( leadHours.getMaximum() );
//
//        if ( latestInclusive.compareTo( latestLeadDurationInclusive ) > 0 )
//        {
//            throw new CalculationException( "Iteration of lead duration bounds failed on the "
//                                            + "iterated upper bound of "
//                                            + latestInclusive
//                                            + " exceeding the declared "
//                                            + "upper bound of "
//                                            + latestLeadDurationInclusive
//                                            + "." );
//        }

        return Pair.of( earliestExclusive, latestInclusive );
    }    
    
    /**
     * <p>Adjusts the input interval to add the "lead offset" to the lower bound. The "lead offset" is the duration by 
     * which the right data must be shifted (or at which rescaling should begin) in order to align with the left data
     * at the desired time scale. For example, if the first left value has a time scale of PT24H and aligns with right
     * forecast data that corresponds to a lead duration interval of (PT18H, PT42H], then the "lead offset" is PT18H. 
     * This offset is only added to intervals that are larger than zero wide. If an interval is zero wide, then a 
     * precise thing has been requested, and there is no scope for adjusting it to ensure pairs begin when they should.  
     * 
     * <p>This adjustment is only necessary within the current retrieval pipeline because the calculation of lead 
     * duration intervals attempts to account for both pairing and pooling at the same time in a precise way, rather
     * than retrieving the maximum amount of data that could be needed for a pool and then forming the pairs from that
     * superset of data.
     * 
     * <p>The whole retrieval process should be reviewed in light of #56213 whose aim is to form pools that are data
     * independent and to populate them with pairs, which are necessarily data dependent, because they are concerned
     * with things like "lead offsets".
     * 
     * @param unadjusted the lead bounds without any accounting for the lead offset
     * @return an adjusted set of lead duration bounds
     * @throws NullPointerException if the input is null
     */
    
    Pair<Duration,Duration> getAdjustedLeadBounds( Pair<Duration, Duration> unadjusted )
    {
        Objects.requireNonNull( unadjusted );
        
        // Something precise requested
        if( unadjusted.getLeft().equals( unadjusted.getRight() ) )
        {
            return unadjusted;
        }

        Feature featureToAdjust = this.getFeature();
        Project projectToAdjust = this.getProject();
        Duration offset = projectToAdjust.getLeadOffset( featureToAdjust );

        // No adjustment needed
        if( Duration.ZERO.equals( offset ) )
        {
            return unadjusted;
        }
        
        // Unadjusted lower bound
        Duration lower = unadjusted.getLeft();
        
        //Unadjusted upper
        Duration upper = unadjusted.getRight();

        if ( !TimeWindow.DURATION_MIN.equals( lower ) )
        {
            lower = lower.plus( offset );
        }
        // If the lower bound, then start at the offset (this assumes no negative lead times)
        else
        {
            lower = offset;
        }

        if ( !TimeWindow.DURATION_MAX.equals( upper ) )
        {
            upper = upper.plus( offset );
        }
        
        Pair<Duration,Duration> adjusted = Pair.of( lower, upper );
        
        LOGGER.debug( "Adding the lead duration offset of {} to the lead duration bounds, changing them from "
                + "{} to {}.", offset, unadjusted, adjusted );
        
        return adjusted;
    }
    
    /**
     * @return the final pooling step
     * @throws CalculationException if the issue pool count could not be determined
     */
    
    int getFinalPoolingStep()
    {
        if (this.finalPoolingStep == null)
        {
            this.finalPoolingStep = this.calculateFinalPoolingStep();
        }
        return this.finalPoolingStep;
    }

    /**
     * @return the final pooling step
     * @throws CalculationException if the issue pool count could not be determined
     */
    
    protected int calculateFinalPoolingStep()
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
        // TODO: Find a way to submit this task without directly calling the database class
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
