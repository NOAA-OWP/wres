package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MeanSquareError}.
 * 
 * @author James Brown
 */
public final class MeanSquareErrorTest
{

    /**
     * Default instance of a {@link MeanSquareError}.
     */

    private MeanSquareError mse;

    @Before
    public void setupBeforeEachTest()
    {
        this.mse = MeanSquareError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.mse.apply( input );

        DoubleScoreMetricComponent metricComponent = MeanSquareError.MAIN.toBuilder()
                                                                         .setUnits( input.getMetadata()
                                                                                         .getMeasurementUnit()
                                                                                         .toString() )
                                                                         .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 400003.929 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanSquareError.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = mse.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.mse.getName().equals( MetricConstants.MEAN_SQUARE_ERROR.toString() ) );
    }


    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.mse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.mse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.mse.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.mse.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.mse.getName() + "'.", actual.getMessage() );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> mse.aggregate( null ) );

        assertEquals( "Specify non-null input to the '" + this.mse.getName() + "'.", actual.getMessage() );
    }

}
