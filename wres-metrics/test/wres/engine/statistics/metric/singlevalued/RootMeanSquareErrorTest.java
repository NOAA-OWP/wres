package wres.engine.statistics.metric.singlevalued;

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
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreMetric.DoubleScoreMetricComponent;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link RootMeanSquareError}.
 * 
 * @author James Brown
 */
public final class RootMeanSquareErrorTest
{

    /**
     * Default instance of a {@link RootMeanSquareError}.
     */

    private RootMeanSquareError rmse;

    @Before
    public void setupBeforeEachTest()
    {
        this.rmse = RootMeanSquareError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.rmse.apply( input );

        DoubleScoreMetricComponent metricComponent = RootMeanSquareError.MAIN.toBuilder()
                                                                             .setUnits( input.getMetadata()
                                                                                             .getMeasurementUnit()
                                                                                             .toString() )
                                                                             .build();

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( metricComponent )
                                                                               .setValue( 632.4586381732801 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( RootMeanSquareError.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = rmse.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.ROOT_MEAN_SQUARE_ERROR.toString(), this.rmse.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.rmse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.rmse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.rmse.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.SUM_OF_SQUARE_ERROR, this.rmse.getCollectionOf() );
    }

    @Test
    public void testHasRealUnits()
    {
        assertTrue( this.rmse.hasRealUnits() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.rmse.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.rmse.getName() + "'.", actual.getMessage() );
    }

}
