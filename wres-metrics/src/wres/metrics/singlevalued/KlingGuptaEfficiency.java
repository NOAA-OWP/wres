package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Collectable;
import wres.metrics.DecomposableScore;
import wres.metrics.FunctionFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <p>Computes the Kling-Gupta Efficiency (KGE) and associated decomposition into correlation, bias and variability.</p>
 *
 * <p>The KGE measures the skill of model predictions against observations in terms of the relative contributions from
 * correlation, bias and variability. The implementation details are described here:</p>
 *
 * <p>Kling, H., Fuchs, M. and Paulin, M. (2012). Runoff conditions in the upper Danube basin under an ensemble of 
 * climate change scenarios. <i>Journal of Hydrology</i>, <b>424-425</b>, pp. 264-277, 
 * DOI:10.1016/j.jhydrol.2012.01.011</p>
 *
 * <p>TODO: add this to a {@link Collectable} with {@link CorrelationPearsons} and have both use a {DoubleScoreOutput}
 * that contains the relevant components for computing both, including the marginal means and variances and the 
 * covariances. Do the same for any other scores that uses these components.
 *
 * @author James Brown
 */
public class KlingGuptaEfficiency extends DecomposableScore<Pool<Pair<Double, Double>>>
{

    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.KLING_GUPTA_EFFICIENCY )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.KLING_GUPTA_EFFICIENCY.getMinimum() )
                                      .setMaximum( MetricConstants.KLING_GUPTA_EFFICIENCY.getMaximum() )
                                      .setOptimum( MetricConstants.KLING_GUPTA_EFFICIENCY.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( KlingGuptaEfficiency.MAIN )
                                                                    .setName( MetricName.KLING_GUPTA_EFFICIENCY )
                                                                    .build();

    /**
     * Default weighting for the correlation term.
     */

    private static final double DEFAULT_RHO_WEIGHT = 1.0;

    /**
     * Default weighting for the variance term.
     */

    private static final double DEFAULT_VAR_WEIGHT = 1.0;

    /**
     * Default weighting for the bias term.
     */

    private static final double DEFAULT_BIAS_WEIGHT = 1.0;

    /**
     * Weighting for the correlation term.
     */

    private final double rhoWeight;

    /**
     * Weighting for the variability term.
     */

    private final double varWeight;

    /**
     * Weighting for the bias term.
     */

    private final double biasWeight;

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static KlingGuptaEfficiency of()
    {
        return new KlingGuptaEfficiency();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        // TODO: implement any required decompositions, based on the instance parameters and return the decomposition
        // template as the componentID in the metadata
        double result = Double.NaN;

        // Compute the components
        double[] leftValues = Slicer.getLeftSide( pool );
        double[] rightValues = Slicer.getRightSide( pool );

        double meanPred = FunctionFactory.mean()
                                         .applyAsDouble( rightValues );
        double meanObs = FunctionFactory.mean()
                                        .applyAsDouble( leftValues );
        double sdPred = FunctionFactory.standardDeviation( meanPred )
                                       .applyAsDouble( rightValues );
        double sdObs = FunctionFactory.standardDeviation( meanObs )
                                      .applyAsDouble( leftValues );

        // Compute from the other components for efficiency
        double numerator = 0.0;
        for ( int i = 0; i < leftValues.length; i++ )
        {
            numerator += ( ( leftValues[i] - meanObs ) * ( rightValues[i] - meanPred ) );
        }

        double rhoVal = numerator / ( sdObs * sdPred * ( leftValues.length - 1 ) );

        // Check for finite correlation
        if ( Double.isFinite( rhoVal ) )
        {
            double gamma = ( sdPred / meanPred ) / ( sdObs / meanObs );
            double beta = meanPred / meanObs;
            double left = Math.pow( this.rhoWeight * ( rhoVal - 1.0 ), 2 );
            double middle = Math.pow( this.varWeight * ( gamma - 1.0 ), 2 );
            double right = Math.pow( this.biasWeight * ( beta - 1.0 ), 2 );
            result = FunctionFactory.finiteOrMissing()
                                    .applyAsDouble( 1.0 - Math.sqrt( left + middle + right ) );
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( KlingGuptaEfficiency.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( KlingGuptaEfficiency.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.KLING_GUPTA_EFFICIENCY;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private KlingGuptaEfficiency()
    {
        super();

        this.rhoWeight = DEFAULT_RHO_WEIGHT;
        this.varWeight = DEFAULT_VAR_WEIGHT;
        this.biasWeight = DEFAULT_BIAS_WEIGHT;
    }

}
