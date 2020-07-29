package wres.engine.statistics.metric.categorical;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.MeasurementUnit;
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
 * <p>
 * The Threat Score or Critical Success Index (CSI) measures the fraction of hits against observed occurrences 
 * (hits + misses) and observed non-occurrences that were predicted incorrectly (false alarms). It measures the 
 * accuracy of a set of predictions at detecting observed occurrences, removing the possibly large number of observed
 * non-occurrences that were predicted correctly.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 */
public class ThreatScore extends ContingencyTableScore
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.THREAT_SCORE )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( 0 )
                                                                                    .setMaximum( 1 )
                                                                                    .setOptimum( 1 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                    .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( ThreatScore.MAIN )
                                                                    .setName( MetricName.THREAT_SCORE )
                                                                    .build();

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static ThreatScore of()
    {
        return new ThreatScore();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final SampleData<Pair<Boolean, Boolean>> s )
    {
        return aggregate( getInputForAggregation( s ) );
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( final DoubleScoreStatisticOuter output )
    {
        is2x2ContingencyTable( output, this );

        double tP = output.getComponent( MetricConstants.TRUE_POSITIVES )
                          .getData()
                          .getValue();

        double fP = output.getComponent( MetricConstants.FALSE_POSITIVES )
                          .getData()
                          .getValue();

        double fN = output.getComponent( MetricConstants.FALSE_NEGATIVES )
                          .getData()
                          .getValue();

        double result = FunctionFactory.finiteOrMissing().applyAsDouble( tP / ( tP + fP + fN ) );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ThreatScore.MAIN )
                                                                               .setValue( result )
                                                                               .build();
        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( ThreatScore.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, output.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.THREAT_SCORE;
    }

    @Override
    public boolean isSkillScore()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private ThreatScore()
    {
        super();
    }

}
