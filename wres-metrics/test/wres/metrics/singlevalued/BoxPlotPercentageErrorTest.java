package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.pools.Pool;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlotPercentageError}.
 * 
 * @author James Brown
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
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        BoxplotStatisticOuter actual = this.boxPlotPercentageError.apply( input );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_PERCENTAGE_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.ERROR_PERCENT_OF_VERIFYING_VALUE )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setUnits( "PERCENT" )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( -60.0, -3.45251092, 1.96168504, 5.88145897, 47.61904762 ) )
                     .build();

        BoxplotStatistic expectedBox = BoxplotStatistic.newBuilder()
                                                       .setMetric( metric )
                                                       .addStatistics( box )
                                                       .build();

        BoxplotStatisticOuter expected = BoxplotStatisticOuter.of( expectedBox, input.getMetadata() );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyAgainstSingleValuedPairsNine()
    {
        //Generate some data
        Pool<TimeSeries<Pair<Double, Double>>> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsNine();

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

            TimeWindow inner = MessageFactory.getTimeWindow( duration, duration );
            TimeWindowOuter window = TimeWindowOuter.of( inner );

            for ( TimeSeries<Pair<Double, Double>> next : input.get() )
            {
                TimeSeries<Pair<Double, Double>> filtered = TimeSeriesSlicer.filter( next, window );
                events.addAll( filtered.getEvents() );
            }

            Pool.Builder<Pair<Double, Double>> builder = new Pool.Builder<>();
            builder.setMetadata( input.getMetadata() );
            for ( Event<Pair<Double, Double>> next : events )
            {
                builder.addData( next.getValue() );
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
                                            .setUnits( "PERCENT" )
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
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        BoxplotStatisticOuter actual = this.boxPlotPercentageError.apply( input );

        PoolMetadata meta = PoolMetadata.of();

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
                                            .setUnits( "PERCENT" )
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
     * Tests for an expected exception on calling {@link BoxPlotPercentageError#apply(Pool)} with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException expected =
                assertThrows( PoolException.class, () -> this.boxPlotPercentageError.apply( null ) );

        assertEquals( "Specify non-null input to the 'BOX PLOT OF PERCENTAGE ERRORS'.", expected.getMessage() );
    }

}
