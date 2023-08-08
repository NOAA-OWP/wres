package wres.metrics.categorical;

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
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Boilerplate;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link PeirceSkillScore}.
 * 
 * @author James Brown
 */
public final class PeirceSkillScoreTest
{

    /**
     * Score used for testing. 
     */

    private PeirceSkillScore pss;

    /**
     * Metadata used for testing.
     */

    private PoolMetadata meta;

    @Before
    public void setUpBeforeEachTest()
    {
        this.pss = PeirceSkillScore.of();
        this.meta = Boilerplate.getPoolMetadata( false );
    }

    @Test
    public void testApplyWithDichotomousInput()
    {
        //Generate some data
        Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.pss.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( PeirceSkillScore.MAIN )
                                                                               .setValue( 0.6347985347985348 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( PeirceSkillScore.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        DoubleScoreStatisticOuter expected = DoubleScoreStatisticOuter.of( score, this.meta );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Boolean, Boolean>> input = Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.pss.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertEquals( MetricConstants.PEIRCE_SKILL_SCORE.toString(), this.pss.getMetricNameString() );
    }

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( this.pss.isDecomposable() );
    }

    @Test
    public void testMetricIsASkillScore()
    {
        assertTrue( this.pss.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.pss.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertSame( MetricConstants.CONTINGENCY_TABLE, this.pss.getCollectionOf() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException exception =
                assertThrows( PoolException.class,
                              () -> this.pss.aggregate( null, null ) );

        String expectedMessage = "Specify non-null input to the '" + this.pss.getMetricNameString() + "'.";

        assertEquals( expectedMessage, exception.getMessage() );
    }

    @Test
    public void testExceptionOnInputThatIsNotSquare()
    {
        DoubleScoreStatistic table = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ContingencyTable.METRIC )
                                                         .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                                      .setMetric( ContingencyTable.TRUE_POSITIVES )
                                                                                                      .setValue( 1.0 ) )
                                                         .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                                      .setMetric( ContingencyTable.TRUE_NEGATIVES )
                                                                                                      .setValue( 1.0 ) )
                                                         .addStatistics( DoubleScoreStatisticComponent.newBuilder()
                                                                                                      .setMetric( ContingencyTable.FALSE_POSITIVES )
                                                                                                      .setValue( 1.0 ) )
                                                         .build();

        DoubleScoreStatisticOuter statistic = DoubleScoreStatisticOuter.of( table, this.meta );

        MetricCalculationException exception =
                assertThrows( MetricCalculationException.class, () -> this.pss.aggregate( statistic, null ) );

        String expectedMessage = "Expected an intermediate result with a square number of elements when computing "
                                 + "the '"
                                 + this.pss.getMetricNameString()
                                 + "': [3].";

        assertEquals( expectedMessage, exception.getMessage() );
    }

}
