package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.TimeSeriesOfPairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfPairs.TimeSeriesOfPairsBuilder;
import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.BoxPlotStatistics;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link BoxPlotPercentageError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BoxPlotPercentageErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link BoxPlotPercentageError}.
     */

    private BoxPlotPercentageError boxPlotPercentageError;

    @Before
    public void setupBeforeEachTest()
    {
        this.boxPlotPercentageError = BoxPlotPercentageError.of();
    }

    @Test
    public void testApplyAgainstSingleValuedPairsOne()
    {
        //Generate some data
        TimeSeriesOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata meta = StatisticMetadata.of( input.getMetadata(),
                                                       input.getRawData().size(),
                                                       MeasurementUnit.of( "%" ),
                                                       MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS,
                                                       MetricConstants.MAIN );

        //Check the results
        BoxPlotStatistics actual = this.boxPlotPercentageError.apply( input );
        VectorOfDoubles probabilities = VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

        List<BoxPlotStatistic> expectedRaw = new ArrayList<>();
        BoxPlotStatistic expectedValue =
                BoxPlotStatistic.of( probabilities,
                                     VectorOfDoubles.of( -60, -3.45251092, 1.96168504, 5.88145897, 47.61904762 ),
                                     MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                     meta );
        expectedRaw.add( expectedValue );

        BoxPlotStatistics expected = BoxPlotStatistics.of( expectedRaw, meta );

        assertEquals( expected.getData().size(), expectedRaw.size() );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyAgainstSingleValuedPairsNine()
    {
        //Generate some data
        TimeSeriesOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsNine();

        //Metadata for the output
        StatisticMetadata meta = StatisticMetadata.of( input.getMetadata(),
                                                       4,
                                                       MeasurementUnit.of( "%" ),
                                                       MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS,
                                                       MetricConstants.MAIN );
        //Check the results        
        List<BoxPlotStatistic> actualRaw = new ArrayList<>();

        // Compute the metric for each duration separately
        SortedSet<Duration> durations = TimeSeriesSlicer.getDurations( input.get(), ReferenceTimeType.DEFAULT );

        for ( Duration duration : durations )
        {
            List<Event<Pair<Double, Double>>> events = TimeSeriesSlicer.filterByDuration( input.get(),
                                                                                          a -> a.equals( duration ),
                                                                                          ReferenceTimeType.DEFAULT );
            TimeSeriesOfPairsBuilder<Double, Double> builder = new TimeSeriesOfPairsBuilder<>();
            builder.setMetadata( input.getMetadata() );
            for ( Event<Pair<Double, Double>> next : events )
            {
                builder.addTimeSeries( TimeSeries.of( new TreeSet<>( Collections.singleton( next ) ) ) );
            }
            actualRaw.addAll( this.boxPlotPercentageError.apply( builder.build() ).getData() );
        }

        BoxPlotStatistics actual = BoxPlotStatistics.of( actualRaw, meta );

        VectorOfDoubles probabilities = VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

        List<BoxPlotStatistic> expectedRaw = new ArrayList<>();

        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -82.18077965,
                                                                  -79.77938695,
                                                                  -69.46445012,
                                                                  -62.09740723,
                                                                  -60.67864584 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -81.5562767,
                                                                  -79.28513708,
                                                                  -69.75900354,
                                                                  -62.24396119,
                                                                  -60.6431853 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -81.29240201,
                                                                  -79.25653082,
                                                                  -70.22129674,
                                                                  -62.39761386,
                                                                  -60.76559307 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -80.66604393,
                                                                  -78.70617858,
                                                                  -70.26934241,
                                                                  -62.75949144,
                                                                  -61.10862116 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -79.69309356,
                                                                  -78.11982986,
                                                                  -70.17251793,
                                                                  -63.4414544,
                                                                  -62.27360684 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -79.70542729,
                                                                  -78.07734424,
                                                                  -69.52150926,
                                                                  -63.32465631,
                                                                  -62.4829006 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -80.10315452,
                                                                  -78.3119715,
                                                                  -69.53725234,
                                                                  -63.66919965,
                                                                  -62.84690545 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -80.48620356,
                                                                  -78.59068418,
                                                                  -69.89792593,
                                                                  -64.38275395,
                                                                  -63.54643 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -80.79870347,
                                                                  -78.8235353,
                                                                  -70.23084269,
                                                                  -64.61547829,
                                                                  -63.63275286 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -80.70585825,
                                                                  -78.67640747,
                                                                  -70.10885291,
                                                                  -63.95325628,
                                                                  -62.72779147 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );
        expectedRaw.add( BoxPlotStatistic.of( probabilities,
                                              VectorOfDoubles.of( -78.91521259,
                                                                  -77.43600293,
                                                                  -70.56698806,
                                                                  -64.27694792,
                                                                  -62.99072983 ),
                                              MetricDimension.ERROR_PERCENT_OF_VERIFYING_VALUE,
                                              meta ) );

        BoxPlotStatistics expected = BoxPlotStatistics.of( expectedRaw, meta );

        assertEquals( expected.getData().size(), expectedRaw.size() );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        BoxPlotStatistics actual = this.boxPlotPercentageError.apply( input );

        StatisticMetadata meta = StatisticMetadata.of( SampleMetadata.of(),
                                                       0,
                                                       MeasurementUnit.of( "%" ),
                                                       MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS,
                                                       MetricConstants.MAIN );

        VectorOfDoubles probabilities = VectorOfDoubles.of( 0.0, 0.25, 0.5, 0.75, 1.0 );
        VectorOfDoubles quantiles = VectorOfDoubles.of( Double.NaN,
                                                        Double.NaN,
                                                        Double.NaN,
                                                        Double.NaN,
                                                        Double.NaN );

        BoxPlotStatistics expected =
                BoxPlotStatistics.of( Collections.singletonList( BoxPlotStatistic.of( probabilities,
                                                                                      quantiles,
                                                                                      meta ) ),
                                      meta );

        assertEquals( expected, actual );
    }

    /**
     * Checks that the {@link BoxPlotPercentageError#getName()} returns 
     * {@link MetricConstants#BOX_PLOT_OF_PERCENTAGE_ERRORS.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( this.boxPlotPercentageError.getName()
                                               .equals( MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS.toString() ) );
    }

    /**
     * Tests for an expected exception on calling {@link BoxPlotPercentageError#apply(SampleData)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        this.exception.expect( SampleDataException.class );
        this.exception.expectMessage( "Specify non-null input to the 'BOX PLOT OF PERCENTAGE ERRORS'." );

        this.boxPlotPercentageError.apply( null );
    }

}
