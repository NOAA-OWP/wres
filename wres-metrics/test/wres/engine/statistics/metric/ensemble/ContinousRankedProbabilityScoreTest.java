package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.BasicPool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricParameterException;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link ContinuousRankedProbabilityScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContinousRankedProbabilityScoreTest
{

    /**
     * Default instance of a {@link ContinuousRankedProbabilityScore}.
     */

    private ContinuousRankedProbabilityScore crps;

    @Before
    public void setupBeforeEachTest()
    {
        this.crps = ContinuousRankedProbabilityScore.of();
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} against expected output
     * where the input contains no missing data.
     */

    @Test
    public void testApplyWithNoMissings()
    {
        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 25.7, Ensemble.of( 23, 43, 45, 23, 54 ) ) );
        pairs.add( Pair.of( 21.4, Ensemble.of( 19, 16, 57, 23, 9 ) ) );
        pairs.add( Pair.of( 32.1, Ensemble.of( 23, 54, 23, 12, 32 ) ) );
        pairs.add( Pair.of( 47.0, Ensemble.of( 12, 54, 23, 54, 78 ) ) );
        pairs.add( Pair.of( 12.1, Ensemble.of( 9, 8, 5, 6, 12 ) ) );
        pairs.add( Pair.of( 43.0, Ensemble.of( 23, 12, 12, 34, 10 ) ) );
        Pool<Pair<Double, Ensemble>> input = BasicPool.of( pairs, PoolMetadata.of() );

        //Metadata for the output
        PoolMetadata m1 = PoolMetadata.of();

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ContinuousRankedProbabilityScore.MAIN )
                                                                               .setValue( 7.63 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} against expected output
     * where the input contains missing data.
     */

    @Test
    public void testApplyWithMissings()
    {

        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 25.7, Ensemble.of( 23, 43, 45, 34.2, 23, 54 ) ) );
        pairs.add( Pair.of( 21.4, Ensemble.of( 19, 16, 57, 23, 9 ) ) );
        pairs.add( Pair.of( 32.1, Ensemble.of( 23, 54, 23, 12, 32, 45.3, 67.1 ) ) );
        pairs.add( Pair.of( 47.0, Ensemble.of( 12, 54, 23, 54 ) ) );
        pairs.add( Pair.of( 12.0, Ensemble.of( 9, 8, 5 ) ) );
        pairs.add( Pair.of( 43.0, Ensemble.of( 23, 12, 12 ) ) );
        Pool<Pair<Double, Ensemble>> input = BasicPool.of( pairs, PoolMetadata.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ContinuousRankedProbabilityScore.MAIN )
                                                                               .setValue( 8.734401927437641 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );

    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} against expected output
     * where the observation falls below the lowest member.
     */

    @Test
    public void testApplyObsMissesLow()
    {
        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 8.0, Ensemble.of( 23, 54, 23, 12, 32 ) ) );
        Pool<Pair<Double, Ensemble>> input = BasicPool.of( pairs, PoolMetadata.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ContinuousRankedProbabilityScore.MAIN )
                                                                               .setValue( 13.36 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} against expected output
     * for a scenario where the observed value overlaps one ensemble member. This exposes a mistake in the Hersbach 
     * (2000) paper where rows 1 and 3 of table/eqn. 26 should be inclusive bounds.
     */

    @Test
    public void testApplyObsEqualsMember()
    {

        //Generate some data
        List<Pair<Double, Ensemble>> pairs = new ArrayList<>();
        pairs.add( Pair.of( 32.0, Ensemble.of( 23, 54, 23, 12, 32 ) ) );
        Pool<Pair<Double, Ensemble>> input = BasicPool.of( pairs, PoolMetadata.of() );

        //Check the results       
        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ContinuousRankedProbabilityScore.MAIN )
                                                                               .setValue( 4.56 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ContinuousRankedProbabilityScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }


    /**
     * Validates the output from {@link ContinuousRankedProbabilityScore#apply(Pool)} when supplied with no 
     * data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Ensemble>> input =
                BasicPool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.crps.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#getName()} returns 
     * {@link MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( crps.getName().equals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( crps.isDecomposable() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( crps.isSkillScore() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#getScoreOutputGroup()} returns the result provided on 
     * construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( crps.getScoreOutputGroup() == MetricGroup.NONE );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isProper()} returns <code>true</code>.
     */

    @Test
    public void testIsProper()
    {
        assertTrue( crps.isProper() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isStrictlyProper()} returns <code>true</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertTrue( crps.isStrictlyProper() );
    }

    /**
     * Tests for an expected exception on calling {@link ContinuousRankedProbabilityScore#apply(Pool)} with 
     * null input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.crps.apply( (Pool<Pair<Double, Ensemble>>) null ) );

        assertEquals( "Specify non-null input to the '" + this.crps.getName() + "'.", actual.getMessage() );
    }


    /**
     * Tests for an expected exception on building a {@link ContinuousRankedProbabilityScore} with 
     * an unrecognized decomposition identifier.
     */

    @Test
    public void testApplyExceptionOnUnrecognizedDecompositionIdentifier()
    {
        MetricParameterException actual = assertThrows( MetricParameterException.class,
                                                   () -> ContinuousRankedProbabilityScore.of( MetricGroup.LBR ) );

        assertEquals( "Unsupported decomposition identifier 'LBR'.", actual.getMessage() );
    }

}
