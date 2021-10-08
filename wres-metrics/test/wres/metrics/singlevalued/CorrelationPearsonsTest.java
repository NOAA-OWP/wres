package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link CorrelationPearsons}.
 * 
 * @author James Brown
 */
public final class CorrelationPearsonsTest
{

    /**
     * Default instance of a {@link CorrelationPearsons}.
     */

    private CorrelationPearsons rho;

    @Before
    public void setupBeforeEachTest()
    {
        this.rho = CorrelationPearsons.of();
    }

    @Test
    public void testApply()
    {
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();
        
        //Compute normally
        DoubleScoreStatisticOuter actual = this.rho.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( CorrelationPearsons.MAIN )
                                                                               .setValue( 0.9999999910148981 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                         .setMetric( CorrelationPearsons.BASIC_METRIC )
                                                         .addStatistics( component )
                                                         .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testAggregate()
    {
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        assertEquals( this.rho.aggregate( rho.getInputForAggregation( input ) ), this.rho.apply( input ) );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        DoubleScoreStatisticOuter actual = rho.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT.toString(), this.rho.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.rho.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.rho.isSkillScore() );
    }

    @Test
    public void testhasRealUnits()
    {
        assertFalse( this.rho.hasRealUnits() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.rho.getScoreOutputGroup() );
    }

    @Test
    public void testGetCollectionOf()
    {
        assertEquals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT, this.rho.getCollectionOf() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.rho.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.rho.getName() + "'.", actual.getMessage() );
    }

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                                   () -> this.rho.aggregate( null ) );

        assertEquals( "Specify non-null input to the '" + this.rho.getName() + "'.", actual.getMessage() );
    }

}
