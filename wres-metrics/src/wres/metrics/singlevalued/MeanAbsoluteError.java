package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Collectable;
import wres.metrics.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;

/**
 * The mean absolute error applies to continuous variables and is the average unsigned difference between a
 * single-valued predictand and verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author James Brown
 */
public class MeanAbsoluteError extends DoubleErrorScore<Pool<Pair<Double, Double>>>
        implements Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{
    /** Canonical description of the metric. */
    public static final DoubleScoreMetric METRIC_INNER =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( 0 )
                                                                       .setMaximum( Double.POSITIVE_INFINITY )
                                                                       .setOptimum( 0 )
                                                                       .setName( ComponentName.MAIN ) )
                             .setName( MetricName.MEAN_ABSOLUTE_ERROR )
                             .build();

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MeanAbsoluteError.class );

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static MeanAbsoluteError of()
    {
        return new MeanAbsoluteError();
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MEAN_ABSOLUTE_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public DoubleScoreStatisticOuter applyIntermediate( DoubleScoreStatisticOuter statistic, Pool<Pair<Double, Double>> pool )
    {
        if ( Objects.isNull( statistic ) )
        {
            throw new PoolException( "Specify a non-null statistic to aggregate when computing the '" + this + "'." );
        }

        return statistic;
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediate( Pool<Pair<Double, Double>> pool )
    {
        LOGGER.debug( "Computing the {}, which may be used as an intermediate statistic for other statistics.",
                      MetricConstants.MEAN_ABSOLUTE_ERROR );

        return super.apply( pool );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.MEAN_ABSOLUTE_ERROR;
    }

    /**
     * Hidden constructor.
     */

    private MeanAbsoluteError()
    {
        super( FunctionFactory.absError(), MeanAbsoluteError.METRIC_INNER );
    }
}
