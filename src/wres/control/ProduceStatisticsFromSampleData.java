package wres.control;

import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.engine.statistics.metric.processing.MetricProcessorForProject;

/**
 * A processor that computes a set of statistics from {@link SampleData}. The statistics are composed as a 
 * {@link StatisticsForProject}.
 */
class ProduceStatisticsFromSampleData implements Function<SampleData<?>, StatisticsForProject>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger( ProduceStatisticsFromSampleData.class );

    /**
     * Processor.
     */

    private final MetricProcessorForProject metricProcessor;

    /**
     * Construct.
     * 
     * @param metricProcessor the metric processor
     * @throws NullPointerException if either input is null
     */

    ProduceStatisticsFromSampleData( final MetricProcessorForProject metricProcessor )
    {
        Objects.requireNonNull( metricProcessor, "Specify a non-null metric processor." );
        this.metricProcessor = metricProcessor;
    }

    @Override
    public StatisticsForProject apply( SampleData<?> input )
    {
        StatisticsForProject returnMe = null;

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
        
        LOGGER.debug( "Completed composing statistics for feature '{}' and time window {}.",
                      input.getMetadata().getIdentifier().getGeospatialID(),
                      input.getMetadata().getTimeWindow() );

        return returnMe;
    }
}
