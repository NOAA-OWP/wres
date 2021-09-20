package wres.engine.statistics.metric.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MissingValues;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <p>The {@link IndexOfAgreement} was proposed by Willmot (1981) to measure the errors of the model predictions 
 * as a proportion of the degree of variability in the predictions and observations from the average observation. 
 * Originally a quadratic score, different exponents may be used in practice. By default, the absolute errors are 
 * computed with an exponent of one, in order to minimize the influence of extreme errors.</p>  
 * <p>Willmott, C. J. 1981. On the validation of models. <i>Physical Geography</i>, <b>2</b>, 184-194</p>
 * 
 * @author James Brown
 */
public class IndexOfAgreement extends DoubleErrorScore<Pool<Pair<Double, Double>>>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.INDEX_OF_AGREEMENT )
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

    public static final DoubleScoreMetric METRIC_INNER = DoubleScoreMetric.newBuilder()
                                                                          .addComponents( IndexOfAgreement.MAIN )
                                                                          .setName( MetricName.INDEX_OF_AGREEMENT )
                                                                          .build();

    /**
     * The default exponent.
     */

    private static final double DEFAULT_EXPONENT = 1.0;

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static IndexOfAgreement of()
    {
        return new IndexOfAgreement();
    }

    /**
     * Exponent. 
     */

    final double exponent;

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> s )
    {
        if ( Objects.isNull( s ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.DOUBLE;

        // Data available
        if ( !s.get().isEmpty() )
        {
            //Compute the average observation
            double oBar = s.get().stream().mapToDouble( Pair::getLeft ).average().getAsDouble();
            //Compute the score
            double numerator = 0.0;
            double denominator = 0.0;
            for ( Pair<Double, Double> nextPair : s.get() )
            {
                numerator += Math.pow( Math.abs( nextPair.getLeft() - nextPair.getRight() ), exponent );
                denominator += ( Math.abs( nextPair.getRight() - oBar )
                                 + Math.pow( Math.abs( nextPair.getLeft() - oBar ), exponent ) );
            }
            returnMe = FunctionFactory.skill().applyAsDouble( numerator, denominator );
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( IndexOfAgreement.MAIN )
                                                                               .setValue( returnMe )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( IndexOfAgreement.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.INDEX_OF_AGREEMENT;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private IndexOfAgreement()
    {
        super( IndexOfAgreement.METRIC_INNER );

        this.exponent = DEFAULT_EXPONENT;
    }

}
