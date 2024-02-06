package wres.metrics.singlevalued;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;

/**
 * The mean error applies to continuous variables and is the average signed difference between a single-valued
 * predictand and a verifying observation. It measures the first-order bias of the predictand.
 *
 * @author James Brown
 */
public class MeanError extends DoubleErrorScore<Pool<Pair<Double, Double>>>
{
    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC_INNER =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( MetricConstants.MEAN_ERROR.getMinimum() )
                                                                       .setMaximum( MetricConstants.MEAN_ERROR.getMaximum() )
                                                                       .setOptimum( MetricConstants.MEAN_ERROR.getOptimum() )
                                                                       .setName( MetricName.MAIN ) )
                             .setName( MetricName.MEAN_ERROR )
                             .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static MeanError of()
    {
        return new MeanError();
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MEAN_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    private MeanError()
    {
        super( MeanError.METRIC_INNER );
    }

}
