package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
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

    @Test
    public void testApply()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

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

    @Test
    public void testApplyWithEvenNumberOfPairs()
    {
        //Generate some data
        List<Pair<Double, Double>> pairs = Arrays.asList( Pair.of( 1.0, 3.0 ),
                                                          Pair.of( 5.0, 9.0 ) );
        SampleData<Pair<Double, Double>> input = SampleDataBasic.of( pairs, SampleMetadata.of() );

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

    @Test
    public void testApplyWithOddNumberOfPairs()
    {
        //Generate some data
        List<Pair<Double, Double>> pairs = Arrays.asList( Pair.of( 0.0, 99999.0 ),
                                                          Pair.of( 12345.6789, 0.0 ),
                                                          Pair.of( 99999.0, 0.0 ) );
        
        SampleData<Pair<Double, Double>> input = SampleDataBasic.of( pairs, SampleMetadata.of() );

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

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = this.medianError.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    @Test
    public void testGetName()
    {
        assertTrue( this.medianError.getName().equals( MetricConstants.MEDIAN_ERROR.toString() ) );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.medianError.isDecomposable() );
    }

    @Test
    public void testIsSkillScore()
    {
        assertFalse( this.medianError.isSkillScore() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( this.medianError.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        this.exception.expect( SampleDataException.class );
        this.exception.expectMessage( "Specify non-null input to the 'MEDIAN ERROR'." );

        this.medianError.apply( null );
    }

}
