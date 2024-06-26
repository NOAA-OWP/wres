package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
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
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link MeanSquareErrorSkillScoreNormalized}.
 * 
 * @author James Brown
 */
public final class MeanSquareErrorSkillScoreNormalizedTest
{

    /**
     * Default instance of a {@link MeanSquareErrorSkillScoreNormalized}.
     */

    private MeanSquareErrorSkillScoreNormalized msessn;

    @Before
    public void setupBeforeEachTest()
    {
        this.msessn = MeanSquareErrorSkillScoreNormalized.of();
    }

    @Test
    public void testApplyWithBaseline()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msessn.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScoreNormalized.MAIN )
                                                                               .setValue( 0.8338214896144127 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( MeanSquareErrorSkillScoreNormalized.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithoutBaseline() throws IOException
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msessn.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScoreNormalized.MAIN )
                                                                               .setValue( 0.8218812203770344 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( MeanSquareErrorSkillScoreNormalized.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithoutBaselineTwo()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.msessn.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( MeanSquareErrorSkillScoreNormalized.MAIN )
                                                                               .setValue( 0.9963778833284114 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( MeanSquareErrorSkillScoreNormalized.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = this.msessn.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED.toString(), this.msessn.getMetricNameString() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.msessn.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertTrue( msessn.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.msessn.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.SUM_OF_SQUARE_ERROR, this.msessn.getCollectionOf() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException expected = assertThrows( PoolException.class, () -> this.msessn.apply( null ) );

        assertEquals( "Specify non-null input to the 'MEAN SQUARE ERROR SKILL SCORE NORMALIZED'.",
                      expected.getMessage() );
    }

}
