package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
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
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataBasic;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlotPercentageError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BoxPlotPercentageErrorTest
{

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
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata meta = StatisticMetadata.of( input.getMetadata(),
                                                       input.getRawData().size(),
                                                       MeasurementUnit.of( "%" ),
                                                       MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS,
                                                       MetricConstants.MAIN );

        BoxplotStatisticOuter actual = this.boxPlotPercentageError.apply( input );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_PERCENTAGE_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.ERROR_PERCENT_OF_VERIFYING_VALUE )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( -60.0, -3.45251092, 1.96168504, 5.88145897, 47.61904762 ) )
                     .build();

        BoxplotStatistic expectedBox = BoxplotStatistic.newBuilder()
                                                       .setMetric( metric )
                                                       .addStatistics( box )
                                                       .build();

        BoxplotStatisticOuter expected = BoxplotStatisticOuter.of( expectedBox, meta );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyAgainstSingleValuedPairsNine()
    {
        //Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsNine();

        //Check the results        
        List<BoxplotStatistic> actualRaw = new ArrayList<>();

        // Compute the metric for each duration separately
        SortedSet<Duration> durations = new TreeSet<>();
        for ( int i = 3; i < 34; i += 3 )
        {
            durations.add( Duration.ofHours( i ) );
        }

        for ( Duration duration : durations )
        {
            List<Event<Pair<Double, Double>>> events = new ArrayList<>();

            TimeWindowOuter window = TimeWindowOuter.of( duration, duration );

            for ( TimeSeries<Pair<Double, Double>> next : input.get() )
            {
                TimeSeries<Pair<Double, Double>> filtered = TimeSeriesSlicer.filter( next, window );
                events.addAll( filtered.getEvents() );
            }

            PoolOfPairsBuilder<Double, Double> builder = new PoolOfPairsBuilder<>();
            builder.setMetadata( input.getMetadata() );
            for ( Event<Pair<Double, Double>> next : events )
            {
                builder.addTimeSeries( TimeSeries.of( MetricTestDataFactory.getBoilerplateMetadata(),
                                                      new TreeSet<>( Collections.singleton( next ) ) ) );
            }

            actualRaw.add( this.boxPlotPercentageError.apply( builder.build() ).getData() );
        }

        List<BoxplotStatistic> expectedRaw = new ArrayList<>();

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_PERCENTAGE_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.ERROR_PERCENT_OF_VERIFYING_VALUE )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -82.18077965,
                                                                                       -79.77938695,
                                                                                       -69.46445012,
                                                                                       -62.09740723,
                                                                                       -60.67864584 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -81.5562767,
                                                                                       -79.28513708,
                                                                                       -69.75900354,
                                                                                       -62.24396119,
                                                                                       -60.6431853 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -81.29240201,
                                                                                       -79.25653082,
                                                                                       -70.22129674,
                                                                                       -62.39761386,
                                                                                       -60.76559307 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -80.66604393,
                                                                                       -78.70617858,
                                                                                       -70.26934241,
                                                                                       -62.75949144,
                                                                                       -61.10862116 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -79.69309356,
                                                                                       -78.11982986,
                                                                                       -70.17251793,
                                                                                       -63.4414544,
                                                                                       -62.27360684 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -79.70542729,
                                                                                       -78.07734424,
                                                                                       -69.52150926,
                                                                                       -63.32465631,
                                                                                       -62.4829006 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -80.10315452,
                                                                                       -78.3119715,
                                                                                       -69.53725234,
                                                                                       -63.66919965,
                                                                                       -62.84690545 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -80.48620356,
                                                                                       -78.59068418,
                                                                                       -69.89792593,
                                                                                       -64.38275395,
                                                                                       -63.54643 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -80.79870347,
                                                                                       -78.8235353,
                                                                                       -70.23084269,
                                                                                       -64.61547829,
                                                                                       -63.63275286 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -80.70585825,
                                                                                       -78.67640747,
                                                                                       -70.10885291,
                                                                                       -63.95325628,
                                                                                       -62.72779147 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -78.91521259,
                                                                                       -77.43600293,
                                                                                       -70.56698806,
                                                                                       -64.27694792,
                                                                                       -62.99072983 ) ) )
                                         .build() );

        assertEquals( expectedRaw, actualRaw );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SampleDataBasic<Pair<Double, Double>> input =
                SampleDataBasic.of( Arrays.asList(), SampleMetadata.of() );

        BoxplotStatisticOuter actual = this.boxPlotPercentageError.apply( input );

        StatisticMetadata meta = StatisticMetadata.of( SampleMetadata.of(),
                                                       0,
                                                       MeasurementUnit.of( "%" ),
                                                       MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS,
                                                       MetricConstants.MAIN );

        List<Double> probabilities = List.of( 0.0, 0.25, 0.5, 0.75, 1.0 );
        List<Double> quantiles = List.of( Double.NaN,
                                          Double.NaN,
                                          Double.NaN,
                                          Double.NaN,
                                          Double.NaN );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_PERCENTAGE_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.ERROR_PERCENT_OF_VERIFYING_VALUE )
                                            .addAllQuantiles( probabilities )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( quantiles )
                     .build();

        BoxplotStatistic expectedBox = BoxplotStatistic.newBuilder()
                                                       .setMetric( metric )
                                                       .addStatistics( box )
                                                       .build();

        BoxplotStatisticOuter expected = BoxplotStatisticOuter.of( expectedBox, meta );

        assertEquals( expected, actual );
    }

    /**
     * Checks that the {@link BoxPlotPercentageError#getName()} returns 
     * {@link MetricConstants#BOX_PLOT_OF_PERCENTAGE_ERRORS.toString()}
     */

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BOX_PLOT_OF_PERCENTAGE_ERRORS.toString(), this.boxPlotPercentageError.getName() );
    }

    /**
     * Tests for an expected exception on calling {@link BoxPlotPercentageError#apply(SampleData)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        SampleDataException expected =
                assertThrows( SampleDataException.class, () -> this.boxPlotPercentageError.apply( null ) );

        assertEquals( "Specify non-null input to the 'BOX PLOT OF PERCENTAGE ERRORS'.", expected.getMessage() );
    }

}
