package wres.metrics.singlevalued;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.metrics.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;

/**
 * The median error applies to continuous variables and is the median signed difference 
 * between a single-valued predictand and a verifying observation. It measures the 
 * median bias of the predictand.
 * 
 * @author James Brown
 */
public class MedianError extends DoubleErrorScore<Pool<Pair<Double, Double>>>
{

    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC_INNER =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( Double.NEGATIVE_INFINITY )
                                                                       .setMaximum( Double.POSITIVE_INFINITY )
                                                                       .setOptimum( 0 )
                                                                       .setName( ComponentName.MAIN ) )
                             .setName( MetricName.MEDIAN_ERROR )
                             .build();

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static MedianError of()
    {
        return new MedianError();
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MEDIAN_ERROR;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    private MedianError()
    {
        super( FunctionFactory.error(), FunctionFactory.median(), MedianError.METRIC_INNER );
    }

}
