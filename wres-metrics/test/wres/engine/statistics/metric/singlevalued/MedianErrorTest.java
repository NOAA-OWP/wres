package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MedianError}.
 * 
 * @author james.brown@hydrosolved.com
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

        DoubleScoreMetricComponent metricComponent = MedianError.METRIC.getComponents( 0 )
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

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithEvenNumberOfPairs()
    {
        //Generate some data
        List<Pair<Double, Double>> pairs = Arrays.asList( Pair.of( 1.0, 3.0 ),
                                                          Pair.of( 5.0, 9.0 ) );
        Pool<Pair<Double, Double>> input = BasicPool.of( pairs, PoolMetadata.of() );

        //Check the results
        DoubleScoreStatisticOuter actual = this.medianError.apply( input );

        DoubleScoreMetricComponent metricComponent = MedianError.METRIC.getComponents( 0 )
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

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithOddNumberOfPairs()
    {
        //Generate some data
        List<Pair<Double, Double>> pairs = Arrays.asList( Pair.of( 0.0, 99999.0 ),
                                                          Pair.of( 12345.6789, 0.0 ),
                                                          Pair.of( 99999.0, 0.0 ) );

        Pool<Pair<Double, Double>> input = BasicPool.of( pairs, PoolMetadata.of() );

        //Check the results
        DoubleScoreStatisticOuter actual = this.medianError.apply( input );

        DoubleScoreMetricComponent metricComponent = MedianError.METRIC.getComponents( 0 )
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

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        BasicPool<Pair<Double, Double>> input =
                BasicPool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.medianError.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.medianError.getName().equals( MetricConstants.MEDIAN_ERROR.toString() ) );
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
        assertTrue( this.medianError.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.medianError.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.medianError.getName() + "'.", actual.getMessage() );
    }

}
