package wres.engine.statistics.metric.singlevalued;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.pools.SampleData;
import wres.engine.statistics.metric.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;

/**
 * The mean absolute error applies to continuous variables and is the average unsigned difference between a
 * single-valued predictand and verifying observation. It measures the first-order bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MeanAbsoluteError extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
{

    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( 0 )
                                                                       .setMaximum( Double.POSITIVE_INFINITY )
                                                                       .setOptimum( 0 )
                                                                       .setName( ComponentName.MAIN ) )
                             .setName( MetricName.MEAN_ABSOLUTE_ERROR )
                             .build();

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

    /**
     * Hidden constructor.
     */

    private MeanAbsoluteError()
    {
        super( FunctionFactory.absError(), MeanAbsoluteError.METRIC );
    }

}
