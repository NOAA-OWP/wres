package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPair;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MedianError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MedianErrorTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link MedianError}.
     */

    private MedianError medianError;

    @Before
    public void setupBeforeEachTest()
    {
        this.medianError = MedianError.of();
    }

    /**
     * Compares the output from {@link MedianError#apply(SingleValuedPairs)} against 
     * expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEDIAN_ERROR,
                                                     MetricConstants.MAIN );
        //Check the results
        DoubleScoreStatistic actual = this.medianError.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 1.0, m1 );
        
        assertEquals( expected, actual );
    }
    
    /**
     * Compares the output from {@link MedianError#apply(SingleValuedPairs)} against 
     * expected output for input that contains an even number of pairs.
     */

    @Test
    public void testApplyWithEvenNumberOfPairs()
    {
        //Generate some data
        List<SingleValuedPair> pairs = Arrays.asList( SingleValuedPair.of( 1, 3 ),
                                                      SingleValuedPair.of( 5, 9 ) );
        SingleValuedPairs input = SingleValuedPairs.of( pairs, SampleMetadata.of() );

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEDIAN_ERROR,
                                                     MetricConstants.MAIN );
        //Check the results
        DoubleScoreStatistic actual = this.medianError.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 3, m1 );
        
        assertEquals( expected, actual );
    }
    
    /**
     * Compares the output from {@link MedianError#apply(SingleValuedPairs)} against 
     * expected output for input that contains an odd number of pairs.
     */

    @Test
    public void testApplyWithOddNumberOfPairs()
    {
        //Generate some data
        List<SingleValuedPair> pairs = Arrays.asList( SingleValuedPair.of( 0, 99999 ),
                                                      SingleValuedPair.of( 12345.6789, 0 ),
                                                      SingleValuedPair.of( 99999, 0 ) );
        SingleValuedPairs input = SingleValuedPairs.of( pairs, SampleMetadata.of() );

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEDIAN_ERROR,
                                                     MetricConstants.MAIN );
        //Check the results
        DoubleScoreStatistic actual = this.medianError.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( -12345.6789, m1 );
        
        assertEquals( expected, actual );
    }     

    /**
     * Validates the output from {@link MeanError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), SampleMetadata.of() );
 
        DoubleScoreStatistic actual = this.medianError.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link MedianError#getName()} returns 
     * {@link MetricConstants#MEDIAN_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( this.medianError.getName().equals( MetricConstants.MEDIAN_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link MedianError#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.medianError.isDecomposable() );
    }

    /**
     * Checks that the {@link MedianError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.medianError.isSkillScore() );
    }

    /**
     * Checks that the {@link MedianError#getScoreOutputGroup()} returns the result 
     * provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.medianError.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link MedianError#apply(SingleValuedPairs)} 
     * with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        this.exception.expect( SampleDataException.class );
        this.exception.expectMessage( "Specify non-null input to the 'MEDIAN ERROR'." );
        
        this.medianError.apply( null );
    }    
  
}
