package wres.metrics.discreteprobability;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.Probability;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
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
        PoolMetadata m1 = Boilerplate.getPoolMetadata( false );
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

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Probability, Probability>> input = Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = brierScore.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BRIER_SCORE.toString(), this.brierScore.getMetricNameString() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.brierScore.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.brierScore.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.brierScore.getScoreOutputGroup() );
    }

    @Test
    public void testIsProper()
    {
        assertTrue( this.brierScore.isProper() );
    }

    @Test
    public void testIsStrictlyProper()
    {
        assertTrue( this.brierScore.isStrictlyProper() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.brierScore.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.brierScore.getMetricNameString() + "'.",
                      actual.getMessage() );
    }
}
