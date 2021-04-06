package wres.engine.statistics.metric.singlevalued.univariate;

import java.util.Objects;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;

/**
 * <p>Computes the minimum value for each side of a pairing. The minimum is computed with the 
 * {@link FunctionFactory#minimum()}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class Minimum extends DecomposableScore<SampleData<Pair<Double, Double>>>
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

    public static Minimum of()
    {
        return new Minimum();
    }

    @Override
    public DoubleScoreStatisticOuter apply( SampleData<Pair<Double, Double>> pairs )
    {
        if ( Objects.isNull( pairs ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        DoubleScoreStatistic score = this.getScore()
                                         .apply( pairs );

        return DoubleScoreStatisticOuter.of( score, pairs.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.MINIMUM;
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

    private Minimum()
    {
        super();

        // Metric
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.MINIMUM )
                                                    .build();

        // Template for the l/r/b components of the score
        DoubleScoreMetricComponent template = DoubleScoreMetricComponent.newBuilder()
                                                                        .setMinimum( Double.NEGATIVE_INFINITY )
                                                                        .setMaximum( Double.POSITIVE_INFINITY )
                                                                        .setOptimum( Double.NaN )
                                                                        .build();

        ToDoubleFunction<VectorOfDoubles> minimum = FunctionFactory.minimum();
        this.score = new UnivariateScore( minimum, metric, template, true );
    }

}
