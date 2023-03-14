package wres.metrics.singlevalued;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.Slicer;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Collectable;
import wres.metrics.FunctionFactory;
import wres.metrics.MetricCollection;
import wres.metrics.Score;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Computes Pearson's product-moment correlation coefficient between the left and right sides of the {SingleValuedPairs}
 * input. Implements {@link Collectable} to avoid repeated calculations of derivative metrics, such as the
 * {@link CoefficientOfDetermination} when both appear in a {@link MetricCollection}.
 * 
 * @author James Brown
 */
public class CorrelationPearsons implements Score<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter>,
        Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
{
    /** Basic description of the metric. */
    public static final DoubleScoreMetric BASIC_METRIC = DoubleScoreMetric.newBuilder()
                                                                          .setName( MetricName.PEARSON_CORRELATION_COEFFICIENT )
                                                                          .build();

    /** Main score component. */
    public static final DoubleScoreMetricComponent MAIN = DoubleScoreMetricComponent.newBuilder()
                                                                                    .setMinimum( -1 )
                                                                                    .setMaximum( 1 )
                                                                                    .setOptimum( 1 )
                                                                                    .setName( ComponentName.MAIN )
                                                                                    .setUnits( MeasurementUnit.DIMENSIONLESS )
                                                                                    .build();

    /** Full description of the metric. */
    public static final DoubleScoreMetric METRIC = DoubleScoreMetric.newBuilder()
                                                                    .addComponents( CorrelationPearsons.MAIN )
                                                                    .setName( MetricName.PEARSON_CORRELATION_COEFFICIENT )
                                                                    .build();

    /** Instance of {@link PearsonsCorrelation}. */
    private final PearsonsCorrelation correlation;

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( CorrelationPearsons.class );

    /**
     * Returns an instance.
     * 
     * @return an instance
     */

    public static CorrelationPearsons of()
    {
        return new CorrelationPearsons();
    }

    @Override
    public DoubleScoreStatisticOuter apply( Pool<Pair<Double, Double>> s )
    {
        LOGGER.debug( "Computing the {}.", MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
        
        if ( Objects.isNull( s ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        double returnMe = Double.NaN;

        // Minimum sample size of 1
        if ( s.get().size() > 1 )
        {
            returnMe = FunctionFactory.finiteOrMissing()
                                      .applyAsDouble( this.correlation.correlation( Slicer.getLeftSide( s ),
                                                                                    Slicer.getRightSide( s ) ) );
        }

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( CorrelationPearsons.MAIN )
                                                                               .setValue( returnMe )
                                                                               .build();

        DoubleScoreStatistic score =
                DoubleScoreStatistic.newBuilder()
                                    .setMetric( CorrelationPearsons.BASIC_METRIC )
                                    .addStatistics( component )
                                    .build();

        return DoubleScoreStatisticOuter.of( score, s.getMetadata() );
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    @Override
    public boolean isDecomposable()
    {
        return false;
    }

    @Override
    public MetricGroup getScoreOutputGroup()
    {
        return MetricGroup.NONE;
    }

    @Override
    public boolean hasRealUnits()
    {
        return false;
    }

    @Override
    public DoubleScoreStatisticOuter aggregate( DoubleScoreStatisticOuter output, Pool<Pair<Double, Double>> pool )
    {
        if ( Objects.isNull( output ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        return output;
    }

    @Override
    public DoubleScoreStatisticOuter getIntermediateStatistic( Pool<Pair<Double, Double>> input )
    {
        LOGGER.debug( "Computing the {}, which may be used as an intermediate statistic for other statistics.",
                      MetricConstants.PEARSON_CORRELATION_COEFFICIENT );

        return this.apply( input );
    }

    @Override
    public MetricConstants getCollectionOf()
    {
        return MetricConstants.PEARSON_CORRELATION_COEFFICIENT;
    }

    @Override
    public String toString()
    {
        return this.getMetricName()
                   .toString();
    }

    /**
     * Hidden constructor.
     */

    CorrelationPearsons()
    {
        super();
        correlation = new PearsonsCorrelation();
    }

}
