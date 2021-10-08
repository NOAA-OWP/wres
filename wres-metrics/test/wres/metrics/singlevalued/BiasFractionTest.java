package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link BiasFraction}.
 * 
 * @author James Brown
 */
public final class BiasFractionTest
{

    /**
     * Default instance of a {@link BiasFraction}.
     */

    private BiasFraction biasFraction;

    @Before
    public void setupBeforeEachTest()
    {
        this.biasFraction = BiasFraction.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        DoubleScoreStatisticOuter actual = this.biasFraction.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( BiasFraction.MAIN )
                                                                               .setValue( 0.056796297974534414 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( BiasFraction.BASIC_METRIC )
                                                            .addStatistics( component )
                                                            .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.biasFraction.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BIAS_FRACTION.toString(), this.biasFraction.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.biasFraction.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.biasFraction.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.biasFraction.getScoreOutputGroup() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.biasFraction.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.biasFraction.getName() + "'.", actual.getMessage() );
    }

}
