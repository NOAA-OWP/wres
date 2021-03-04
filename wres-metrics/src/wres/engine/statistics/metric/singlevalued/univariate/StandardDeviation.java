package wres.engine.statistics.metric.singlevalued.univariate;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.DecomposableScore;
import wres.engine.statistics.metric.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;

/**
 * <p>Computes the standard deviation for each side of a pairing. The standard deviation is computed with the 
 * {@link FunctionFactory#standardDeviation()}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class StandardDeviation extends DecomposableScore<SampleData<Pair<Double, Double>>>
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

    public static StandardDeviation of()
    {
        return new StandardDeviation();
    }

    @Override
    public DoubleScoreStatisticOuter apply( SampleData<Pair<Double, Double>> pairs )
    {
        if ( Objects.isNull( pairs ) )
        {
            throw new SampleDataException( "Specify non-null input to the '" + this + "'." );
        }

        DoubleScoreStatistic score = this.getScore().apply( pairs );

        return DoubleScoreStatisticOuter.of( score, pairs.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.STANDARD_DEVIATION;
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

    private StandardDeviation()
    {
        super();

        // Metric
        DoubleScoreMetric metric = DoubleScoreMetric.newBuilder()
                                                    .setName( MetricName.STANDARD_DEVIATION )
                                                    .build();

        // Template for the l/r/b components of the score
        DoubleScoreMetricComponent template = DoubleScoreMetricComponent.newBuilder()
                                                                        .setMinimum( 0 )
                                                                        .setMaximum( Double.POSITIVE_INFINITY )
                                                                        .setOptimum( Double.NaN )
                                                                        .build();

        this.score = new UnivariateScore( FunctionFactory.standardDeviation(), metric, template, true );
    }

}
