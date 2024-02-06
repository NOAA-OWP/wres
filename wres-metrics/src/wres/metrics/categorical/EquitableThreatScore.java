package wres.metrics.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * The Equitable Threat Score (ETS) is a dichotomous measure of the fraction of all predicted outcomes that occurred
 * (i.e. were true positives), after factoring out the correct predictions that were due to chance.
 *
 * @author James Brown
 */
public class EquitableThreatScore extends ContingencyTableScore
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.EQUITABLE_THREAT_SCORE )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.EQUITABLE_THREAT_SCORE.getMinimum() )
                                      .setMaximum( MetricConstants.EQUITABLE_THREAT_SCORE.getMaximum() )
                                      .setOptimum( MetricConstants.EQUITABLE_THREAT_SCORE.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC =
            DoubleScoreMetric.newBuilder()
                             .addComponents( EquitableThreatScore.MAIN )
                             .setName( MetricName.EQUITABLE_THREAT_SCORE )
                             .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static EquitableThreatScore of()
    {
        return new EquitableThreatScore();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Boolean, Boolean>> pool )
    {
        return applyIntermediate( this.getIntermediate( pool ), pool );
    }

    @Override
    public DoubleScoreStatisticOuter applyIntermediate( final DoubleScoreStatisticOuter output,
                                                        Pool<Pair<Boolean, Boolean>> pool )
    {
        this.is2x2ContingencyTable( output, this );

        double tP = output.getComponent( MetricConstants.TRUE_POSITIVES )
                          .getStatistic()
                          .getValue();

        double fP = output.getComponent( MetricConstants.FALSE_POSITIVES )
                          .getStatistic()
                          .getValue();

        double fN = output.getComponent( MetricConstants.FALSE_NEGATIVES )
                          .getStatistic()
                          .getValue();

        double tN = output.getComponent( MetricConstants.TRUE_NEGATIVES )
                          .getStatistic()
                          .getValue();

        final double t = tP + fP + fN;
        final double hitsRandom = ( ( tP + fN ) * ( tP + fP ) ) / ( t + tN );
        double result = FunctionFactory.finiteOrMissing()
                                       .applyAsDouble( ( tP - hitsRandom ) / ( t - hitsRandom ) );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( EquitableThreatScore.MAIN )
                                                                               .setValue( result )
                                                                               .build();
        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( EquitableThreatScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        return DoubleScoreStatisticOuter.of( score, output.getPoolMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.EQUITABLE_THREAT_SCORE;
    }

    /**
     * Hidden constructor.
     */

    private EquitableThreatScore()
    {
        super();
    }

}
