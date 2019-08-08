package wres.control;

import java.util.Objects;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.EnsemblePairs;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.StatisticsForProjectBuilder;
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
     * Returns an instance.
     * 
     * @param metricProcessor the metric processor
     * @return an instance of the producer
     */
    
    static ProduceStatisticsFromSampleData of( MetricProcessorForProject metricProcessor )
    {
        return new ProduceStatisticsFromSampleData( metricProcessor );
    }
    
    @Override
    public StatisticsForProject apply( SampleData<?> pool )
    {
        Objects.requireNonNull( pool );
        
        StatisticsForProject returnMe = null;
        
        // No data in the composition
        if ( pool.getRawData().isEmpty()
             && ( !pool.hasBaseline() || pool.getBaselineData().getRawData().isEmpty() ) )
        {
            LOGGER.debug( "Empty pool discovered for {}: no statistics will be produced.", pool.getMetadata() );
            
            StatisticsForProjectBuilder builder = new StatisticsForProjectBuilder();
            
            // Empty container
            return builder.build();
        }

        // Process the pairs
        if ( pool instanceof SingleValuedPairs )
        {
            returnMe = this.metricProcessor.getMetricProcessorForSingleValuedPairs().apply( (SingleValuedPairs) pool );
        }
        else if ( pool instanceof EnsemblePairs )
        {
            returnMe = this.metricProcessor.getMetricProcessorForEnsemblePairs().apply( (EnsemblePairs) pool );
        }
        else
        {
            throw new WresProcessingException( "While processing pairs: encountered an unexpected type of pairs." );
        }

        LOGGER.debug( "Completed composing statistics for feature '{}' and time window {}.",
                      pool.getMetadata().getIdentifier().getGeospatialID(),
                      pool.getMetadata().getTimeWindow() );

        return returnMe;
    }
    

    /**
     * Construct.
     * 
     * @param metricProcessor the metric processor
     * @throws NullPointerException if either input is null
     */

    private ProduceStatisticsFromSampleData( MetricProcessorForProject metricProcessor )
    {
        Objects.requireNonNull( metricProcessor, "Specify a non-null metric processor." );
        
        this.metricProcessor = metricProcessor;
    }
    
}
