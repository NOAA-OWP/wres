package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MeanSquareErrorSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanSquareErrorSkillScoreTest
{

    /**
     * Default instance of a {@link MeanSquareErrorSkillScore}.
     */

    private MeanSquareErrorSkillScore msess;

    @Before
    public void setupBeforeEachTest()
    {
        this.msess = MeanSquareErrorSkillScore.of();
    }

    @Test
    public void testApplyWithBaseline()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msess.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScore.MAIN )
                                                                               .setValue( 0.8007025335093799 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanSquareErrorSkillScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithoutBaseline() throws IOException
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msess.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScore.MAIN )
                                                                               .setValue( 0.7832791707526114 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanSquareErrorSkillScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithoutBaselineTwo()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msess.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScore.MAIN )
                                                                               .setValue( 0.9963647159052861 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanSquareErrorSkillScore.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = this.msess.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.msess.getName().equals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.msess.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertTrue( this.msess.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.msess.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE, this.msess.getCollectionOf() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException expected = assertThrows( PoolException.class, () -> this.msess.apply( null ) );

        assertEquals( "Specify non-null input to the 'MEAN SQUARE ERROR SKILL SCORE'.", expected.getMessage() );
    }

}
