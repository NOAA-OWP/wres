package wres.metrics.ensemble;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.types.Ensemble;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ContinuousRankedProbabilitySkillScore}.
 * 
 * @author James Brown
 */
public final class ContinousRankedProbabilitySkillScoreTest
{
    /**
     * Default instance of a {@link ContinuousRankedProbabilitySkillScore}.
     */

    private ContinuousRankedProbabilitySkillScore crpss;

    @Before
    public void setupBeforeEachTest()
    {
        this.crpss = ContinuousRankedProbabilitySkillScore.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 25.7, Ensemble.of( 23, 43, 45, 23, 54 ) ) );
        pairs.add( Pair.of( 21.4, Ensemble.of( 19, 16, 57, 23, 9 ) ) );
        pairs.add( Pair.of( 32.1, Ensemble.of( 23, 54, 23, 12, 32 ) ) );
        pairs.add( Pair.of( 47.0, Ensemble.of( 12, 54, 23, 54, 78 ) ) );
        pairs.add( Pair.of( 12.0, Ensemble.of( 9, 8, 5, 6, 12 ) ) );
        pairs.add( Pair.of( 43.0, Ensemble.of( 23, 12, 12, 34, 10 ) ) );
        List<Pair<Double, Ensemble>> basePairs = new ArrayList<>();
        basePairs.add( Pair.of( 25.7, Ensemble.of( 20, 43, 45, 23, 94 ) ) );
        basePairs.add( Pair.of( 21.4, Ensemble.of( 19, 76, 57, 23, 9 ) ) );
        basePairs.add( Pair.of( 32.1, Ensemble.of( 23, 53, 23, 12, 32 ) ) );
        basePairs.add( Pair.of( 47.0, Ensemble.of( 2, 54, 23, 54, 78 ) ) );
        basePairs.add( Pair.of( 12.1, Ensemble.of( 9, 18, 5, 6, 12 ) ) );
        basePairs.add( Pair.of( 43.0, Ensemble.of( 23, 12, 12, 39, 10 ) ) );

        Pool<Pair<Double, Ensemble>> input = Pool.of( pairs,
                                                      PoolMetadata.of(),
                                                      basePairs,
                                                      PoolMetadata.of( true ),
                                                      null );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crpss.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ContinuousRankedProbabilitySkillScore.MAIN )
                                                                               .setValue( 0.0779168348809044 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( ContinuousRankedProbabilitySkillScore.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Ensemble>> input =
                Pool.of( List.of(),
                         PoolMetadata.of(),
                         List.of(),
                         PoolMetadata.of( true ),
                         null );

        DoubleScoreStatisticOuter actual = this.crpss.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.toString(), this.crpss.getMetricNameString() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.crpss.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertTrue( this.crpss.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.crpss.getScoreOutputGroup() );
    }

    @Test
    public void testIsProper()
    {
        assertFalse( this.crpss.isProper() );
    }

    @Test
    public void testIsStrictlyProper()
    {
        assertFalse( this.crpss.isStrictlyProper() );
    }

    @Test
    public void testMetadataContainsBaselineIdentifier() throws IOException
    {
        Pool<Pair<Double, Ensemble>> pairs = MetricTestDataFactory.getEnsemblePairsOne();

        assertEquals( "ESP",
                      this.crpss.apply( pairs )
                                .getPoolMetadata()
                                .getEvaluation()
                                .getBaselineDataName() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.crpss.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.crpss.getMetricNameString() + "'.", actual.getMessage() );
    }

    @Test
    public void testExceptionOnInputWithMissingBaseline()
    {
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 25.7, Ensemble.of( 23, 43, 45, 23, 54 ) ) );
        Pool<Pair<Double, Ensemble>> input = Pool.of( pairs, PoolMetadata.of() );

        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.crpss.apply( input ) );

        assertEquals( "Specify a non-null baseline for the 'CONTINUOUS RANKED PROBABILITY SKILL SCORE'.",
                      actual.getMessage() );
    }

}
