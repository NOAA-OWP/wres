package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DoubleScoreStatistic.DoubleScoreStatisticComponent;

/**
 * Tests the {@link BiasFraction}.
 * 
 * @author james.brown@hydrosolved.com
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
        SampleData<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

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
        SampleData<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatisticOuter actual = this.biasFraction.apply( input );

        assertEquals( Double.NaN, actual.getComponent( MetricConstants.MAIN ).getData().getValue(), 0.0 );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.biasFraction.getName().equals( MetricConstants.BIAS_FRACTION.toString() ) );
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
        assertTrue( this.biasFraction.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        SampleDataException actual = assertThrows( SampleDataException.class,
                                                   () -> this.biasFraction.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.biasFraction.getName() + "'.", actual.getMessage() );
    }

}
