package wres.metrics.discreteprobability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.Probability;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Boilerplate;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link BrierScore}.
 * 
 * @author James Brown
 */
public final class BrierScoreTest
{

    /**
     * Default instance of a {@link BrierScore}.
     */

    private BrierScore brierScore;

    @Before
    public void setupBeforeEachTest()
    {
        this.brierScore = BrierScore.of();
    }

    /**
     * Compares the output from {@link BrierScore#apply(Pool)} against expected output.
     */

    @Test
    public void testApply()
    {
        // Generate some data
        Pool<Pair<Probability, Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        // Metadata for the output
        PoolMetadata m1 = Boilerplate.getPoolMetadata();
        // Check the results       
        DoubleScoreStatisticOuter actual = this.brierScore.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( BrierScore.MAIN )
                                                                               .setValue( 0.26 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( BrierScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    /**
     * Validates the output from {@link BrierScore#apply(Pool)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Probability, Probability>> input = Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = brierScore.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    /**
     * Checks that the {@link BrierScore#getName()} returns {@link MetricConstants.BRIER_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BRIER_SCORE.toString(), this.brierScore.getName() );
    }

    /**
     * Checks that the {@link BrierScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.brierScore.isDecomposable() );
    }

    /**
     * Checks that the {@link BrierScore#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.brierScore.isSkillScore() );
    }

    /**
     * Checks that the {@link BrierScore#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.brierScore.getScoreOutputGroup() );
    }

    /**
     * Checks that the {@link BrierScore#isProper()} returns <code>true</code>.
     */

    @Test
    public void testIsProper()
    {
        assertTrue( this.brierScore.isProper() );
    }

    /**
     * Checks that the {@link BrierScore#isStrictlyProper()} returns <code>true</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertTrue( this.brierScore.isStrictlyProper() );
    }

    /**
     * Tests for an expected exception on calling {@link BrierScore#apply(DiscreteProbabilityPairs)} with null 
     * input.
     */

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.brierScore.apply( (Pool<Pair<Probability, Probability>>) null ) );

        assertEquals( "Specify non-null input to the '" + this.brierScore.getName() + "'.", actual.getMessage() );
    }

}
