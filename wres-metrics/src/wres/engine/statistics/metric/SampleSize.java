package wres.engine.statistics.metric;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
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
 * @author james.brown@hydrosolved.com
 */
class SampleSize<S extends Pool<?>> extends OrdinaryScore<S, DoubleScoreStatisticOuter>
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
    public DoubleScoreStatisticOuter apply( S s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        LOGGER.trace( "Found {} pairs in the input to the {} for '{}'.",
                      s.getRawData().size(),
                      this.getName(),
                      s.getMetadata() );

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( SampleSize.BASIC_METRIC )
                                    .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                 .setMetric( SampleSize.MAIN )
                                                                                 .setValue( (double) s.getRawData()
                                                                                                      .size() ) )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, s.getMetadata() );
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

    /**
     * Hidden constructor.
     */

    private SampleSize()
    {
        super();
    }

}
