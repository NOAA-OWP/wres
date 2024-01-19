package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.MissingValues;
import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.FunctionFactory;
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

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.INDEX_OF_AGREEMENT.getMinimum() )
                                      .setMaximum( MetricConstants.INDEX_OF_AGREEMENT.getMaximum() )
                                      .setOptimum( MetricConstants.INDEX_OF_AGREEMENT.getOptimum() )
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
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = MissingValues.DOUBLE;

        // Data available
        if ( !pool.get().isEmpty() )
        {
            //Compute the average observation
            double oBar = pool.get()
                              .stream()
                              .mapToDouble( Pair::getLeft )
                              .average().orElse( Double.NaN );

            if ( !Double.isNaN( oBar ) )
            {
                //Compute the score
                double numerator = 0.0;
                double denominator = 0.0;
                for ( Pair<Double, Double> nextPair : pool.get() )
                {
                    numerator += Math.pow( Math.abs( nextPair.getLeft() - nextPair.getRight() ), exponent );
                    denominator += ( Math.abs( nextPair.getRight() - oBar )
                                     + Math.pow( Math.abs( nextPair.getLeft() - oBar ), exponent ) );
                }
                returnMe = FunctionFactory.skill()
                                          .applyAsDouble( numerator, denominator );
            }
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

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
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
