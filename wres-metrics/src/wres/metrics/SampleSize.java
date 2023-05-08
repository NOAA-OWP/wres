package wres.metrics;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.MetricName;

/**
 * Constructs a {@link Metric} that returns the sample size.
 * 
 * @author James Brown
 */
class SampleSize<S extends Pool<?>> implements Score<S, DoubleScoreStatisticOuter>
{
    /**
     * Default logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SampleSize.class );

    /**
     * A basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.SAMPLE_SIZE )
                                                                          .build();

    /**
     * The main score component.
     */

    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( Double.POSITIVE_INFINITY )
                                                                                    .setOptimum( Double.POSITIVE_INFINITY )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .setUnits( "COUNT" )
                                                                                    .build();
    /**
     * A full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( SampleSize.MAIN )
                                                                    .setName( MetricName.SAMPLE_SIZE )
                                                                    .build();

    /**
     * Returns an instance.
     * 
     * @param <S> the input type
     * @return an instance
     */

    public static <S extends Pool<?>> SampleSize<S> of()
    {
        return new SampleSize<>();
    }

    @Override
    public DoubleScoreStatisticOuter apply( S pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        LOGGER.trace( "Found {} pairs in the input to the {} for '{}'.",
                      pool.get().size(),
                      this.getMetricNameString(),
                      pool.getMetadata() );

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( SampleSize.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( SampleSize.MAIN )
                                                                                 .setValue( pool.get()
                                                                                                .size() ) )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.SAMPLE_SIZE;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public String toString()
    {
        return this.getMetricName()
                   .toString();
    }

    /**
     * Hidden constructor.
     */

    private SampleSize()
    {
        super();
    }

}
