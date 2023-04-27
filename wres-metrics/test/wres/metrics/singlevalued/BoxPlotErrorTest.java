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

import wres.datamodel.pools.MeasurementUnit;
import wres.datamodel.pools.Pool;
import wres.config.MetricConstants;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeWindowOuter;
import wres.metrics.MetricTestDataFactory;
import wres.statistics.MessageFactory;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.BoxplotMetric.LinkedValueType;
import wres.statistics.generated.BoxplotMetric.QuantileValueType;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * Tests the {@link BoxPlotError}.
 * 
 * @author James Brown
 */
public final class BoxPlotErrorTest
{

    private static final String CMS = "CMS";
    
    /**
     * Default instance of a {@link BoxPlotError}.
     */

    private BoxPlotError boxPlotError;

    @Before
    public void setupBeforeEachTest()
    {
        this.boxPlotError = BoxPlotError.of();
    }

    @Test
    public void testApplyAgainstSingleValuedPairsOne()
    {
        //Generate some data
        Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsOne();

        BoxplotStatisticOuter actual = this.boxPlotError.apply( input );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setUnits( MeasurementUnit.DIMENSIONLESS )
                                            .build();

        Box box = Box.newBuilder()
                     .addAllQuantiles( List.of( -3.0, -0.325, 1.0, 2.55, 2000.0 ) )
                     .build();

        BoxplotStatistic expected = BoxplotStatistic.newBuilder()
                                                    .setMetric( metric )
                                                    .addStatistics( box )
                                                    .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyAgainstSingleValuedPairsNine()
    {
        //Generate some data
        Pool<TimeSeries<Pair<Double, Double>>> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsNine();

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

            actualRaw.add( this.boxPlotError.apply( builder.build() ).getData() );
        }

        List<BoxplotStatistic> expectedRaw = new ArrayList<>();

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( List.of( 0.0, 0.25, 0.5, 0.75, 1.0 ) )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setUnits( CMS )
                                            .build();

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -387.33,
                                                                                       -384.665,
                                                                                       -361.67,
                                                                                       -339.17,
                                                                                       -336.67 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -396.0,
                                                                                       -395.1675,
                                                                                       -376.67,
                                                                                       -352.165,
                                                                                       -349.33 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -407.33,
                                                                                       -406.83,
                                                                                       -392.0,
                                                                                       -365.17,
                                                                                       -360.67 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -422.67,
                                                                                       -421.335,
                                                                                       -408.33,
                                                                                       -378.33,
                                                                                       -371.33 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -447.33,
                                                                                       -442.33,
                                                                                       -422.0,
                                                                                       -389.67,
                                                                                       -380.67 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -461.33,
                                                                                       -453.4975,
                                                                                       -429.335,
                                                                                       -404.67,
                                                                                       -396.67 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -475.33,
                                                                                       -467.33,
                                                                                       -441.33,
                                                                                       -420.835,
                                                                                       -414.67 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -493.33,
                                                                                       -485.665,
                                                                                       -456.0,
                                                                                       -443.33,
                                                                                       -441.33 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -512.67,
                                                                                       -505.835,
                                                                                       -475.33,
                                                                                       -460.335,
                                                                                       -458.67 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -516.67,
                                                                                       -512.335,
                                                                                       -486.665,
                                                                                       -473.0025,
                                                                                       -472.67 ) ) )
                                         .build() );

        expectedRaw.add( BoxplotStatistic.newBuilder()
                                         .setMetric( metric )
                                         .addStatistics( Box.newBuilder()
                                                            .addAllQuantiles( List.of( -529.33,
                                                                                       -525.83,
                                                                                       -502.33,
                                                                                       -478.83,
                                                                                       -475.33 ) ) )
                                         .build() );

        assertEquals( expectedRaw, actualRaw );
    }

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        Pool<Pair<Double, Double>> input =
                Pool.of( Arrays.asList(), PoolMetadata.of() );

        BoxplotStatisticOuter actual = this.boxPlotError.apply( input );

        PoolMetadata meta = PoolMetadata.of();

        List<Double> probabilities = List.of( 0.0, 0.25, 0.5, 0.75, 1.0 );
        List<Double> quantiles = List.of( Double.NaN,
                                          Double.NaN,
                                          Double.NaN,
                                          Double.NaN,
                                          Double.NaN );

        BoxplotMetric metric = BoxplotMetric.newBuilder()
                                            .setName( MetricName.BOX_PLOT_OF_ERRORS )
                                            .setLinkedValueType( LinkedValueType.NONE )
                                            .setQuantileValueType( QuantileValueType.FORECAST_ERROR )
                                            .addAllQuantiles( probabilities )
                                            .setMinimum( Double.NEGATIVE_INFINITY )
                                            .setMaximum( Double.POSITIVE_INFINITY )
                                            .setUnits( "DIMENSIONLESS" )
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

    @Test
    public void testGetName()
    {
        assertEquals( MetricConstants.BOX_PLOT_OF_ERRORS.toString(), this.boxPlotError.getName() );
    }

    @Test
    public void testApplyExceptionOnNullInput()
    {
        PoolException expected = assertThrows( PoolException.class, () -> this.boxPlotError.apply( null ) );

        assertEquals( "Specify non-null input to the 'BOX PLOT OF ERRORS'.", expected.getMessage() );
    }

}
