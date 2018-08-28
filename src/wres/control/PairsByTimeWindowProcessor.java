package wres.control;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.engine.statistics.metric.processing.MetricProcessorException;
import wres.engine.statistics.metric.processing.MetricProcessorForProject;

/**
 * Task that computes a set of metric results for a particular time window.
 */
class PairsByTimeWindowProcessor implements Supplier<StatisticsForProject>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger( PairsByTimeWindowProcessor.class );

    /**
     * The future metric input.
     */
    private final Future<SampleData<?>> futureInput;

    /**
     * Processor.
     */

    private final MetricProcessorForProject metricProcessor;

    /**
     * Construct.
     * 
     * @param futureInput the future metric input
     * @param metricProcessor the metric processor
     * @throws NullPointerException if either input is null
     */

    PairsByTimeWindowProcessor( final Future<SampleData<?>> futureInput,
                                final MetricProcessorForProject metricProcessor )
    {
        Objects.requireNonNull( futureInput, "Specify a non-null input for the processor." );
        Objects.requireNonNull( metricProcessor, "Specify a non-null metric processor." );
        this.futureInput = futureInput;
        this.metricProcessor = metricProcessor;
    }

    @Override
    public StatisticsForProject get()
    {
        StatisticsForProject returnMe = null;
        try
        {
            SampleData<?> input = futureInput.get();
            LOGGER.debug( "Completed processing of pairs for feature '{}' and time window {}.",
                          input.getMetadata().getIdentifier().getGeospatialID(),
                          input.getMetadata().getTimeWindow() );
            // Process the pairs
            if ( input instanceof SingleValuedPairs )
            {
                returnMe = metricProcessor.getMetricProcessorForSingleValuedPairs().apply( (SingleValuedPairs) input );
            }
            else if ( input instanceof EnsemblePairs )
            {
                returnMe = metricProcessor.getMetricProcessorForEnsemblePairs().apply( (EnsemblePairs) input );
            }
            else
            {
                throw new WresProcessingException( "While processing pairs: encountered an unexpected type of pairs." );
            }
        }
        catch ( InterruptedException e )
        {
            LOGGER.warn( "Interrupted while processing pairs:", e );
            Thread.currentThread().interrupt();
        }
        catch ( ExecutionException | MetricProcessorException e )
        {
            throw new WresProcessingException( "While processing pairs:", e );
        }
        return returnMe;
    }
}
