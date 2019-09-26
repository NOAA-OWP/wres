package wres.control;

import java.util.Objects;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.Ensemble;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfPairs.TimeSeriesOfPairsBuilder;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.statistics.StatisticsForProject;
import wres.datamodel.statistics.StatisticsForProject.StatisticsForProjectBuilder;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
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
        if ( pool instanceof TimeSeriesOfSingleValuedPairs )
        {
            // Temporary mapping between two different worlds in order to allow progress for #56214            
            TimeSeriesOfSingleValuedPairs in = (TimeSeriesOfSingleValuedPairs) pool;

            TimeSeriesOfPairsBuilder<Double, Double> builder = new TimeSeriesOfPairsBuilder<>();
            for ( TimeSeries<SingleValuedPair> next : in.get() )
            {
                builder.addTimeSeries( TimeSeriesSlicer.transform( next,
                                                                   pair -> Pair.of( pair.getLeft(),
                                                                                    pair.getRight() ) ) );
            }
            builder.setMetadata( in.getMetadata() ).setClimatology( in.getClimatology() );
            if ( in.hasBaseline() )
            {
                for ( TimeSeries<SingleValuedPair> next : in.getBaselineData().get() )
                {
                    builder.addTimeSeriesForBaseline( TimeSeriesSlicer.transform( next,
                                                                       pair -> Pair.of( pair.getLeft(),
                                                                                        pair.getRight() ) ) );
                }

                builder.setMetadataForBaseline( in.getBaselineData().getMetadata() );
            }
            
            returnMe = this.metricProcessor.getMetricProcessorForSingleValuedPairs().apply( builder.build() );
        }
        else if ( pool instanceof TimeSeriesOfEnsemblePairs )
        {
            // Temporary mapping between two different worlds in order to allow progress for #56214            
            TimeSeriesOfEnsemblePairs in = (TimeSeriesOfEnsemblePairs) pool;

            TimeSeriesOfPairsBuilder<Double, Ensemble> builder = new TimeSeriesOfPairsBuilder<>();
            for ( TimeSeries<EnsemblePair> next : in.get() )
            {
                builder.addTimeSeries( TimeSeriesSlicer.transform( next,
                                                                   pair -> Pair.of( pair.getLeft(),
                                                                                    pair.getRight() ) ) );
            }
            builder.setMetadata( in.getMetadata() ).setClimatology( in.getClimatology() );
            if ( in.hasBaseline() )
            {
                for ( TimeSeries<EnsemblePair> next : in.getBaselineData().get() )
                {
                    builder.addTimeSeriesForBaseline( TimeSeriesSlicer.transform( next,
                                                                       pair -> Pair.of( pair.getLeft(),
                                                                                        pair.getRight() ) ) );
                }

                builder.setMetadataForBaseline( in.getBaselineData().getMetadata() );
            }
            
            returnMe = this.metricProcessor.getMetricProcessorForEnsemblePairs().apply( builder.build() );
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
