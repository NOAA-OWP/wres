package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.Slicer;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link BoxPlotError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BoxPlotErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link BoxPlotError}.
     */

    private BoxPlotError boxPlotError;

    @Before
    public void setupBeforeEachTest()
    {
        this.boxPlotError = BoxPlotError.of();
    }

    /**
     * Compares the output from {@link BoxPlotError#apply(SingleValuedPairs)} against 
     * expected output for the fake data from {@link MetricTestDataFactory#getSingleValuedPairsOne()}.
     */

    @Test
    public void testApplyAgainstSingleValuedPairsOne()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata meta = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                       input.getRawData().size(),
                                                       MeasurementUnit.of(),
                                                       MetricConstants.BOX_PLOT_OF_ERRORS,
                                                       MetricConstants.MAIN );
        //Check the results
        BoxPlotStatistics actual = this.boxPlotError.apply( input );
        VectorOfDoubles probabilities = VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

        List<BoxPlotStatistic> expectedRaw = new ArrayList<>();
        BoxPlotStatistic bp =
                BoxPlotStatistic.of( probabilities,
                                     VectorOfDoubles.of( -3, -0.325, 1, 2.55, 2000 ),
                                     meta );
        expectedRaw.add( bp );

        BoxPlotStatistics expected = BoxPlotStatistics.of( expectedRaw, meta );
        
        assertEquals( expected.getData().size(), expectedRaw.size() );
        
        assertEquals( expected, actual );
    }

    /**
     * Compares the output from {@link BoxPlotError#apply(SingleValuedPairs)} against 
     * expected output for the fake data from {@link MetricTestDataFactory#getSingleValuedPairsNine()}.
     */

    @Test
    public void testApplyAgainstSingleValuedPairsNine()
    {
        //Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsNine();

        //Metadata for the output
        StatisticMetadata meta = StatisticMetadata.of( input.getMetadata(),
                                                       4,
                                                       input.getMetadata().getMeasurementUnit(),
                                                       MetricConstants.BOX_PLOT_OF_ERRORS,
                                                       MetricConstants.MAIN );
        //Check the results        
        List<BoxPlotStatistic> actualRaw = new ArrayList<>();

        // Compute the metric for each duration separately
        input.getDurations()
             .forEach( next -> actualRaw.addAll( this.boxPlotError.apply( Slicer.filterByDuration( input,
                                                                                                a -> a.equals( next ) ) )
                                                               .getData() ) );

        BoxPlotStatistics actual = BoxPlotStatistics.of( actualRaw, meta );
        
        VectorOfDoubles probabilities = VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 );
        
        List<BoxPlotStatistic> expectedRaw = new ArrayList<>();
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -387.33, -384.665, -361.67, -339.17, -336.67 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -396.0, -395.1675, -376.67, -352.165, -349.33 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -407.33, -406.83, -392, -365.17, -360.67 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -422.67, -421.335, -408.33, -378.33, -371.33 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -447.33, -442.33, -422, -389.67, -380.67 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -461.33, -453.4975, -429.335, -404.67, -396.67 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -475.33, -467.33, -441.33, -420.835, -414.67 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -493.33, -485.665, -456, -443.33, -441.33 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -512.67, -505.835, -475.33, -460.335, -458.67 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -516.67, -512.335, -486.665, -473.0025, -472.67 ),
                                         meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                         VectorOfDoubles.of( -529.33, -525.83, -502.33, -478.83, -475.33 ),
                                         meta ) );

        BoxPlotStatistics expected = BoxPlotStatistics.of( expectedRaw, meta );
        
        assertEquals( expected.getData().size(), expectedRaw.size() );
        
        assertEquals( expected, actual );
    }

    /**
     * Validates the output from {@link BoxPlotError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), SampleMetadata.of() );

        BoxPlotStatistics actual = boxPlotError.apply( input );

        assertTrue( actual.getData().isEmpty() );
    }

    /**
     * Checks that the {@link BoxPlotError#getName()} returns 
     * {@link MetricConstants#BOX_PLOT_OF_ERRORS.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( boxPlotError.getName().equals( MetricConstants.BOX_PLOT_OF_ERRORS.toString() ) );
    }

    /**
     * Tests for an expected exception on calling {@link BoxPlotError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'BOX PLOT OF ERRORS'." );

        boxPlotError.apply( null );
    }

}
