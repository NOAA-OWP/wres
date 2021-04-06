package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
 * Tests the {@link KlingGuptaEfficiency}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class KlingGuptaEfficiencyTest
{

    /**
     * Default instance of a {@link KlingGuptaEfficiency}.
     */

    private KlingGuptaEfficiency kge;

    @Before
    public void setupBeforeEachTest()
    {
        this.kge = KlingGuptaEfficiency.of();
    }

    @Test
    public void testApply() throws IOException
    {
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Check the results
        DoubleScoreStatisticOuter actual = this.kge.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( KlingGuptaEfficiency.MAIN )
                                                                               .setValue( 0.8921704394462281 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( KlingGuptaEfficiency.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyTwo()
    {
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Check the results
        DoubleScoreStatisticOuter actual = this.kge.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( KlingGuptaEfficiency.MAIN )
                                                                               .setValue( 0.9432025316651065 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( KlingGuptaEfficiency.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = this.kge.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.kge.getName().equals( MetricConstants.KLING_GUPTA_EFFICIENCY.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertTrue( this.kge.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertTrue( this.kge.isSkillScore() );
    }

    @Test
    public void testhasRealUnits()
    {
        assertFalse( this.kge.hasRealUnits() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.kge.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.kge.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.kge.getName() + "'.", actual.getMessage() );
    }

}
