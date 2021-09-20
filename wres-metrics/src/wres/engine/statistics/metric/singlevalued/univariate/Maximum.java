package wres.engine.statistics.metric.singlevalued.univariate;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;

/**
 * <p>Computes the maximum value for each side of a pairing. The maximum is computed with the 
 * {@link FunctionFactory#maximum()}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class Maximum extends DecomposableScore<Pool<Pair<Double, Double>>>
{

    /**
     * The scoring rule.
     */

    private final UnivariateScore score;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static Maximum of()
    {
        return new Maximum();
    }

    @Override
    public DoubleScoreStatisticOuter apply( Pool<Pair<Double, Double>> pairs )
    {
        if ( Objects.isNull( pairs ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        DoubleScoreStatistic scoreStatistic = this.getScore()
                                         .apply( pairs );

        return DoubleScoreStatisticOuter.of( scoreStatistic, pairs.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MAXIMUM;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.UNIVARIATE_STATISTIC;
    }

    /**
     * @return the scoring rule.
     */

    private UnivariateScore getScore()
    {
        return this.score;
    }

    /**
     * Hidden constructor.
     */

    private Maximum()
    {
        super();

        // Metric
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.MAXIMUM )
                                                    .build();

        // Template for the l/r/b components of the score
        DoubleScoreMetricComponent template = DoubleScoreMetricComponent.newBuilder()
                                                                        .setMinimum( Double.NEGATIVE_INFINITY )
                                                                        .setMaximum( Double.POSITIVE_INFINITY )
                                                                        .setOptimum( Double.NaN )
                                                                        .build();

        ToDoubleFunction<VectorOfDoubles> maximum = FunctionFactory.maximum();
        this.score = new UnivariateScore( maximum, metric, template, true );
    }

}
