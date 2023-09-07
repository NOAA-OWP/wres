package wres.metrics.categorical;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

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
 * Tests the {@link ThreatScore}.
 *
 * @author James Brown
 */
public final class ThreatScoreTest
{

    /**
     * Score used for testing. 
     */

    private ThreatScore ts;

    /**
     * Metadata used for testing.
     */

    private PoolMetadata meta;

    @Before
    public void setUpBeforeEachTest()
    {
        this.ts = ThreatScore.of();
        meta = Boilerplate.getPoolMetadata( false );
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Boolean, Boolean>> input = MetricTestDataFactory.getDichotomousPairsOne();

        //Check the resulthis.ts
        DoubleScoreStatisticOuter actual = this.ts.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( ThreatScore.MAIN )
                                                                               .setValue( 0.5734265734265734 )
                                                                               .build();

        DoubleScoreStatistic score = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( ThreatScore.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = this.ts.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testMetricIsNamedCorrectly()
    {
        assertEquals( MetricConstants.THREAT_SCORE.toString(), this.ts.getMetricNameString() );
    }

    @Test
    public void testMetricIsNotDecoposable()
    {
        assertFalse( this.ts.isDecomposable() );
    }

    @Test
    public void testMetricIsASkillScore()
    {
        assertFalse( this.ts.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.ts.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertSame( MetricConstants.CONTINGENCY_TABLE, this.ts.getCollectionOf() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        MetricCalculationException actual = assertThrows( MetricCalculationException.class,
                                                          () -> this.ts.applyIntermediate( null, null ) );

        assertEquals( "Specify non-null input to the '" + this.ts.getMetricNameString() + "'.", actual.getMessage() );
    }
}
