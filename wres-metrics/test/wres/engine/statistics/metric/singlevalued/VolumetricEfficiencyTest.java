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
import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataBasic;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.pools.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link VolumetricEfficiency}.
 * 
 * @author james.brown@hydrosolved.com
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
        SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsFive();

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
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = this.ve.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.ve.getName().equals( MetricConstants.VOLUMETRIC_EFFICIENCY.toString() ) );
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
        assertTrue( this.ve.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        SampleDataException actual = assertThrows( SampleDataException.class, () -> this.ve.apply( null ) );

        assertEquals( "Specify non-null input to the 'VOLUMETRIC EFFICIENCY'.", actual.getMessage() );
    }

}
