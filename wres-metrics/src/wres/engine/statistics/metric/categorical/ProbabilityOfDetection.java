package wres.engine.statistics.metric.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * The Probability of Detection (PoD) measures the fraction of observed occurrences that were hits.
 * 
 * @author james.brown@hydrosolved.com
 */
public class ProbabilityOfDetection extends ContingencyTableScore
{

    /**
     * Canonical description of the metric.
     */

    public static final DoubleScoreMetric METRIC =
            DoubleScoreMetric.newBuilder()
                             .addComponents( DoubleScoreMetricComponent.newBuilder()
                                                                       .setMinimum( 0 )
                                                                       .setMaximum( 1 )
                                                                       .setOptimum( 1 )
                                                                       .setName( ComponentName.MAIN ) )
                             .setName( MetricName.PROBABILITY_OF_DETECTION )
                             .build();

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static ProbabilityOfDetection of()
    {
        return new ProbabilityOfDetection();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final SampleData<Pair<Boolean, Boolean>> s )
    {
        return aggregate( this.getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( final DoubleScoreStatisticOuter output )
    {
        this.is2x2ContingencyTable( output, this );

        double tP = output.getComponent( MetricConstants.TRUE_POSITIVES )
                          .getData()
                          .getValue();

        double fN = output.getComponent( MetricConstants.FALSE_NEGATIVES )
                          .getData()
                          .getValue();

        double result = FunctionFactory.finiteOrMissing().applyAsDouble( tP / ( tP + fN ) );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setName( ComponentName.MAIN )
                                                                               .setValue( result )
                                                                               .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ProbabilityOfDetection.METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, output.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.PROBABILITY_OF_DETECTION;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private ProbabilityOfDetection()
    {
        super();
    }
}
