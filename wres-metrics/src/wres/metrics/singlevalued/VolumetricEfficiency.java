package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConstants;
import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * <p>The {@link VolumetricEfficiency} (VE) accumulates the absolute observations (VO) and, separately, it accumulates 
 * the absolute errors of the predictions (VP). It then expresses the difference between the two as a fraction of the 
 * accumulated observations, i.e. VE = (VO - VP) / VO.</p> 
 *
 * <p>A score of 1 denotes perfect efficiency and a score of 0 denotes a VP that matches the VO. The lower bound of 
 * the measure is <code>-Inf</code> and a score below zero indicates a VP that exceeds the VO.</p>
 *
 * @author James Brown
 */
public class VolumetricEfficiency extends DoubleErrorScore<Pool<Pair<Double, Double>>>
{
    /**
     * Basic description of the metric.
     */

    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.VOLUMETRIC_EFFICIENCY )
                                                                          .build();

    /**
     * Main score component.
     */

    public static final DoubleScoreMetricComponent MAIN =
            DoubleScoreMetricComponent.newBuilder()
                                      .setMinimum( MetricConstants.VOLUMETRIC_EFFICIENCY.getMinimum() )
                                      .setMaximum( MetricConstants.VOLUMETRIC_EFFICIENCY.getMaximum() )
                                      .setOptimum( MetricConstants.VOLUMETRIC_EFFICIENCY.getOptimum() )
                                      .setName( MetricName.MAIN )
                                      .setUnits( MeasurementUnit.DIMENSIONLESS )
                                      .build();

    /**
     * Full description of the metric.
     */

    public static final DoubleScoreMetric METRIC_INNER = DoubleScoreMetric.newBuilder()
                                                                          .addComponents( VolumetricEfficiency.MAIN )
                                                                          .setName( MetricName.VOLUMETRIC_EFFICIENCY )
                                                                          .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static VolumetricEfficiency of()
    {
        return new VolumetricEfficiency();
    }

    @Override
    public DoubleScoreStatisticOuter apply( final Pool<Pair<Double, Double>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }
        Double vO = 0.0;
        double vP = 0.0;
        for ( Pair<Double, Double> nextPair : pool.get() )
        {
            vO += Math.abs( nextPair.getLeft() );
            vP += Math.abs( nextPair.getLeft() - nextPair.getRight() );
        }

        double result = Double.NaN;

        //Compute the atomic errors in a stream
        if ( !vO.equals( 0.0 ) )
        {
            result = ( vO - vP ) / vO;
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( VolumetricEfficiency.MAIN )
                                                                               .setValue( result )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( VolumetricEfficiency.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, pool.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.VOLUMETRIC_EFFICIENCY;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    /**
     * Hidden constructor.
     */

    private VolumetricEfficiency()
    {
        super( VolumetricEfficiency.METRIC_INNER );
    }

}
