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
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.Boilerplate;
import wres.metrics.MetricCalculationException;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link EquitableThreatScore}.
 *
 * @author James Brown
 */
public final class EquitableThreatScoreTest
{

    /**
     * Score used for testing. 
     */

    private EquitableThreatScore ets;

    /**
     * Metadata used for testing.
     */

    private PoolMetadata meta;

    @Before
    public void setUpBeforeEachTest()
    {
        this.ets = EquitableThreatScore.of();
        this.meta = Boilerplate.getPoolMetadata( false );
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.ets.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( EquitableThreatScore.MAIN )
                                                                               .setValue( 0.43768152544513195 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( EquitableThreatScore.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = this.ets.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertEquals( MetricConstants.EQUITABLE_THREAT_SCORE.toString(), this.ets.getMetricNameString() );
    }

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( ets.isDecomposable() );
    }

    @Test
    public void testMetricIsASkillScore()
    {
        assertTrue( ets.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.ets.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertSame( MetricConstants.CONTINGENCY_TABLE, this.ets.getCollectionOf() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        MetricCalculationException actual = assertThrows( MetricCalculationException.class,
                                                          () -> this.ets.applyIntermediate( null, null ) );

        assertEquals( "Specify non-null input to the '" + this.ets.getMetricNameString() + "'.", actual.getMessage() );
    }

}
