package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.BasicPool;
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
 * @author james.brown@hydrosolved.com
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
        BasicPool<Pair<Double, Double>> input =
                BasicPool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = rmse.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( rmse.getName().equals( MetricConstants.ROOT_MEAN_SQUARE_ERROR.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( rmse.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( rmse.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( rmse.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( rmse.getCollectionOf().equals( MetricConstants.SUM_OF_SQUARE_ERROR ) );
    }

    @Test
    public void testHasRealUnits()
    {
        assertTrue( rmse.hasRealUnits() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.rmse.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.rmse.getName() + "'.", actual.getMessage() );
    }

}
