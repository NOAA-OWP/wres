package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

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
 * Tests the {@link CoefficientOfDetermination}.
 * 
 * @author James Brown
 */
public final class CoefficientOfDeterminationTest
{

    /**
     * Default instance of a {@link CoefficientOfDetermination}.
     */

    private CoefficientOfDetermination cod;

    @Before
    public void setupBeforeEachTest()
    {
        this.cod = CoefficientOfDetermination.of();
    }

    @Test
    public void testApply()
    {
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Compute normally
        DoubleScoreStatisticOuter actual = this.cod.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( CoefficientOfDetermination.MAIN )
                                                                               .setValue( 0.9999999820297963 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( CoefficientOfDetermination.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getStatistic() );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( List.of(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = this.cod.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getStatistic().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.COEFFICIENT_OF_DETERMINATION.toString(), this.cod.getMetricNameString() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.cod.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.cod.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.cod.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, this.cod.getCollectionOf() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.cod.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.cod.getMetricNameString() + "'.", actual.getMessage() );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {

        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.cod.aggregate( null, null ) );

        assertEquals( "Specify non-null input to the '" + this.cod.getMetricNameString() + "'.", actual.getMessage() );
    }

}
