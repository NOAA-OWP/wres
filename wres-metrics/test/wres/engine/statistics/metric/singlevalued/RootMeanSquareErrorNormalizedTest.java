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
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link RootMeanSquareErrorNormalized}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class RootMeanSquareErrorNormalizedTest
{

    /**
     * Default instance of a {@link RootMeanSquareErrorNormalized}.
     */

    private RootMeanSquareErrorNormalized rmsen;

    @Before
    public void setupBeforeEachTest()
    {
        this.rmsen = RootMeanSquareErrorNormalized.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.rmsen.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( RootMeanSquareErrorNormalized.MAIN )
                                                                               .setValue( 0.05719926297814069 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( RootMeanSquareErrorNormalized.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = this.rmsen.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.ROOT_MEAN_SQUARE_ERROR_NORMALIZED.toString(), this.rmsen.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.rmsen.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.rmsen.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.rmsen.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testHasRealUnits()
    {
        assertFalse( this.rmsen.hasRealUnits() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException expected = assertThrows( PoolException.class, () -> this.rmsen.apply( null ) );

        assertEquals( "Specify non-null input to the 'ROOT MEAN SQUARE ERROR NORMALIZED'.", expected.getMessage() );
    }

}
