package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MeanError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanErrorTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link MeanError}.
     */

    private MeanError meanError;

    @Before
    public void setupBeforeEachTest()
    {
        this.meanError = MeanError.of();
    }

    @Test
    public void testApply()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_ERROR,
                                                     MetricConstants.MAIN );
        //Check the results
        final DoubleScoreStatistic actual = this.meanError.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 200.55, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );
 
        DoubleScoreStatistic actual = this.meanError.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.meanError.getName().equals( MetricConstants.MEAN_ERROR.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.meanError.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.meanError.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.meanError.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        this.exception.expect( SampleDataException.class );
        this.exception.expectMessage( "Specify non-null input to the 'MEAN ERROR'." );
        
        this.meanError.apply( null );
    }    
  
}
