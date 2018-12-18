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
import wres.util.TimeHelper;

abstract class SampleDataIterator implements Iterator<Future<SampleData<?>>>
{
    protected static final String NEWLINE = System.lineSeparator();

    private final Feature feature;

    private final Project project;
    LeftHandCache leftCache;
    private VectorOfDoubles climatology;
    private Integer finalPoolingStep;
    private Path outputPathForPairs;
    private final Queue<OrderedSampleMetadata> sampleMetadata = new LinkedList<>(  );

    void addSample( final OrderedSampleMetadata orderedSampleMetadata)
    {
        this.sampleMetadata.add( orderedSampleMetadata );
        this.getLogger().debug(
                "Evaluating {}: {}",
                ConfigHelper.getFeatureDescription( this.getFeature() ),
                orderedSampleMetadata
        );
    }

    int amountOfSamplesLeft()
    {
        return this.sampleMetadata.size();
    }

    protected Feature getFeature()
    {
        return this.feature;
    }

    protected Project getProject()
    {
        return this.project;
    }

    VectorOfDoubles getClimatology() throws IOException
    {
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

    SampleDataIterator( Feature feature,
                        Project project,
                        Path outputPathForPairs,
                        final Collection<OrderedSampleMetadata> sampleMetadata)
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

        if (sampleMetadata == null || sampleMetadata.size() == 0)
        {
            try
            {
                this.calculateSamples();
            }
            catch ( CalculationException e )
            {
                throw new IOException( "The time windows to evaluate could not be calculated.", e );
            }
        }
        else
        {
            this.sampleMetadata.addAll( sampleMetadata );
        }

        ProgressMonitor.setSteps( (long)this.amountOfSamplesLeft() );
    }

    /**
     * Generates a list of TimeWindows that will generate SampleData objects
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

        Long offset;
        try
        {
            offset = this.getProject().getLeadOffset( this.getFeature() ).longValue();
        }
        catch ( IOException | SQLException e )
        {
            throw new CalculationException( "The offset between observed values and "
                                            + "forecasted values are needed to determine "
                                            + "when a lead range should begin, but could "
                                            + "not be loaded.",
                                            e );
        }

        beginning = this.getProject().getLeadFrequency().multipliedBy( sampleNumber ).plus( offset, TimeHelper.LEAD_RESOLUTION );
        end = beginning.plus( this.getProject().getLeadPeriod() );

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
