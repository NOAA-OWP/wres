package wres.engine.statistics.metric.singlevalued;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.engine.statistics.metric.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;

/**
 * The median error applies to continuous variables and is the median signed difference 
 * between a single-valued predictand and a verifying observation. It measures the 
 * median bias of the predictand.
 * 
 * @author james.brown@hydrosolved.com
 */
public class MedianError extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
{

    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC =
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
        super( FunctionFactory.error(), FunctionFactory.median(), MedianError.METRIC );
    }

}
