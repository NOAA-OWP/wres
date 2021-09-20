package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
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
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link IndexOfAgreement}.
 * 
 * @author James Brown
 */
public final class IndexOfAgreementTest
{

    /**
     * Default instance of a {@link IndexOfAgreement}.
     */

    private IndexOfAgreement ioa;

    @Before
    public void setupBeforeEachTest()
    {
        this.ioa = IndexOfAgreement.of();
    }

    @Test
    public void testApply() throws IOException
    {
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Check the results
        DoubleScoreStatisticOuter actual = this.ioa.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( IndexOfAgreement.MAIN )
                                                                               .setValue( 0.8221179993380173 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( IndexOfAgreement.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = ioa.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.INDEX_OF_AGREEMENT.toString(), this.ioa.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.ioa.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.ioa.isSkillScore() );
    }

    @Test
    public void testhasRealUnits()
    {
        assertFalse( this.ioa.hasRealUnits() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.ioa.getScoreOutputGroup() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class, () -> this.ioa.apply( null ) );

        assertEquals( "Specify non-null input to the 'INDEX OF AGREEMENT'.", actual.getMessage() );
    }

}
