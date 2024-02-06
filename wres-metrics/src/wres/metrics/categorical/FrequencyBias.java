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
 * Measures the predicted fraction of occurrences against the observed fraction of occurrences. A ratio of 1.0 
 * indicates an absence of any bias in the predicted and observed frequencies with which an event occurs.
 *
 * @author James Brown
 */
public class FrequencyBias extends ContingencyTableScore
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.FREQUENCY_BIAS )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.FREQUENCY_BIAS.getMinimum() )
                                      .setMaximum( MetricConstants.FREQUENCY_BIAS.getMaximum() )
                                      .setOptimum( MetricConstants.FREQUENCY_BIAS.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( FrequencyBias.MAIN )
                                                                    .setName( MetricName.FREQUENCY_BIAS )
                                                                    .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static FrequencyBias of()
    {
        return new FrequencyBias();
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

        final double value =
                FunctionFactory.finiteOrMissing().applyAsDouble( ( tP + fP ) / ( tP + fN ) );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( FrequencyBias.MAIN )
                                                                               .setValue( value )
                                                                               .build();
        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( FrequencyBias.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        return DoubleScoreStatisticOuter.of( score, output.getPoolMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.FREQUENCY_BIAS;
    }

    /**
     * Hidden constructor.
     */

    private FrequencyBias()
    {
        super();
    }

}
