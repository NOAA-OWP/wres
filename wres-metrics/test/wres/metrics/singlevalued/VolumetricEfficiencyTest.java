package wres.metrics.singlevalued;

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
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link VolumetricEfficiency}.
 * 
 * @author James Brown
 */
public final class VolumetricEfficiencyTest
{

    /**
     * Default instance of a {@link VolumetricEfficiency}.
     */

    private VolumetricEfficiency ve;

    @Before
    public void setupBeforeEachTest()
    {
        this.ve = VolumetricEfficiency.of();
    }

    @Test
    public void testApply() throws IOException
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Check the results
        DoubleScoreStatisticOuter actual = this.ve.apply( input );

        DoubleScoreStatisticComponent component = DoubleScoreStatisticComponent.newBuilder()
                                                                               .setMetric( VolumetricEfficiency.MAIN )
                                                                               .setValue( 0.657420176533252 )
                                                                               .build();

        DoubleScoreStatistic expected = DoubleScoreStatistic.newBuilder()
                                                            .setMetric( VolumetricEfficiency.BASIC_METRIC )
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

        DoubleScoreStatisticOuter actual = this.ve.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.VOLUMETRIC_EFFICIENCY.toString(), this.ve.getName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.ve.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.ve.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.ve.getScoreOutputGroup() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class, () -> this.ve.apply( null ) );

        assertEquals( "Specify non-null input to the 'VOLUMETRIC EFFICIENCY'.", actual.getMessage() );
    }

}
