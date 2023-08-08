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

import wres.datamodel.Probability;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Boilerplate;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link BrierSkillScore}.
 *
 * @author James Brown
 */
public final class BrierSkillScoreTest
{
    /**
     * Default instance of a {@link BrierSkillScore}.
     */

    private BrierSkillScore brierSkillScore;

    @Before
    public void setupBeforeEachTest()
    {
        this.brierSkillScore = BrierSkillScore.of();
    }

    @Test
    public void testApplyWithSuppliedBaseline()
    {
        // Generate some data
        Pool<Pair<Probability, Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        // Metadata for the output
        PoolMetadata m1 = Boilerplate.getPoolMetadata( false );

        // Check the results       
        DoubleScoreStatisticOuter actual = this.brierSkillScore.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( BrierSkillScore.MAIN )
                                                                               .setValue( 0.11363636363636376 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( BrierSkillScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected.getStatistic(), actual.getStatistic() );
    }

    @Test
    public void testApplyWithClimatologicalBaseline()
    {
        // Generate some data
        Pool<Pair<Probability, Probability>> input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        // Metadata for the output
        PoolMetadata m1 = Boilerplate.getPoolMetadata( false );

        // Check the results
        DoubleScoreStatisticOuter actual = this.brierSkillScore.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( BrierSkillScore.MAIN )
                                                                               .setValue( -0.040000000000000036 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( BrierSkillScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }


    /**
     * Validates the output from {@link BrierSkillScore#apply(Pool)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Probability, Probability>> input = Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = brierSkillScore.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BRIER_SKILL_SCORE.toString(), this.brierSkillScore.getMetricNameString() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.brierSkillScore.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertTrue( this.brierSkillScore.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.brierSkillScore.getScoreOutputGroup() );
    }

    @Test
    public void testIsProper()
    {
        assertFalse( this.brierSkillScore.isProper() );
    }

    @Test
    public void testIsStrictlyProper()
    {
        assertFalse( this.brierSkillScore.isStrictlyProper() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.brierSkillScore.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.brierSkillScore.getMetricNameString() + "'.",
                      actual.getMessage() );
    }

    @Test
    public void testApplyNaNOutputWithNaNBaseline()
    {
        assertEquals( Double.NaN,
                      this.brierSkillScore.apply( MetricTestDataFactory.getDiscreteProbabilityPairsFour() )
                                          .getComponent( MetricConstants.MAIN )
                                          .getStatistic()
                                          .getValue(),
                      0.0 );
    }
}
