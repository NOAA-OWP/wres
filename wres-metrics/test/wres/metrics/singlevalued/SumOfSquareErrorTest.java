package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link SumOfSquareError}.
 * 
 * @author James Brown
 */
public final class SumOfSquareErrorTest
{

    /**
     * Default instance of a {@link SumOfSquareError}.
     */

    private SumOfSquareError sse;

    @Before
    public void setupBeforeEachTest()
    {
        this.sse = SumOfSquareError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        DoubleScoreStatisticOuter actual = this.sse.apply( input );

        DoubleScoreMetricComponent metricComponent = SumOfSquareError.MAIN.toBuilder()
                                                                          .setUnits( input.getMetadata()
                                                                                          .getMeasurementUnit()
                                                                                          .toString() )
                                                                          .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 4000039.29 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( SumOfSquareError.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .setSampleSize( 10 )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = sse.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.SUM_OF_SQUARE_ERROR.toString(), this.sse.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.sse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.sse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.sse.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.SUM_OF_SQUARE_ERROR, this.sse.getCollectionOf() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.sse.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.sse.getName() + "'.", actual.getMessage() );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.sse.aggregate( null ) );

        assertEquals( "Specify non-null input to the '" + this.sse.getName() + "'.", actual.getMessage() );
    }

}
