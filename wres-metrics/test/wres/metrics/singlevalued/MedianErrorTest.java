package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MedianError}.
 * 
 * @author James Brown
 */
public final class MedianErrorTest
{

    /**
     * Default instance of a {@link MedianError}.
     */

    private MedianError medianError;

    @Before
    public void setupBeforeEachTest()
    {
        this.medianError = MedianError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.medianError.apply( input );

        DoubleScoreMetricComponent metricComponent = MedianError.METRIC_INNER.getComponents( 0 )
                                                                       .toBuilder()
                                                                       .setUnits( input.getMetadata()
                                                                                       .getMeasurementUnit()
                                                                                       .toString() )
                                                                       .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 1 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( DoubleScoreMetric.newBuilder()
                                                                                      .setName( MetricName.MEDIAN_ERROR ) )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithEvenNumberOfPairs()
    {
        //Generate some data
        List<Pair<Double, Double>> pairs = Arrays.asList( Pair.of( 1.0, 3.0 ),
                                                          Pair.of( 5.0, 9.0 ) );
        Pool<Pair<Double, Double>> input = Pool.of( pairs, PoolMetadata.of() );

        //Check the results
        DoubleScoreStatisticOuter actual = this.medianError.apply( input );

        DoubleScoreMetricComponent metricComponent = MedianError.METRIC_INNER.getComponents( 0 )
                                                                       .toBuilder()
                                                                       .setUnits( input.getMetadata()
                                                                                       .getMeasurementUnit()
                                                                                       .toString() )
                                                                       .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 3 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( DoubleScoreMetric.newBuilder()
                                                                                      .setName( MetricName.MEDIAN_ERROR ) )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithOddNumberOfPairs()
    {
        //Generate some data
        List<Pair<Double, Double>> pairs = Arrays.asList( Pair.of( 0.0, 99999.0 ),
                                                          Pair.of( 12345.6789, 0.0 ),
                                                          Pair.of( 99999.0, 0.0 ) );

        Pool<Pair<Double, Double>> input = Pool.of( pairs, PoolMetadata.of() );

        //Check the results
        DoubleScoreStatisticOuter actual = this.medianError.apply( input );

        DoubleScoreMetricComponent metricComponent = MedianError.METRIC_INNER.getComponents( 0 )
                                                                       .toBuilder()
                                                                       .setUnits( input.getMetadata()
                                                                                       .getMeasurementUnit()
                                                                                       .toString() )
                                                                       .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( -12345.6789 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( DoubleScoreMetric.newBuilder()
                                                                                      .setName( MetricName.MEDIAN_ERROR ) )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.medianError.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.MEDIAN_ERROR.toString(), this.medianError.getMetricNameString() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.medianError.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.medianError.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE,  this.medianError.getScoreOutputGroup() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.medianError.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.medianError.getMetricNameString() + "'.", actual.getMessage() );
    }

}
