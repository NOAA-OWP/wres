package wres.datamodel.time;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.types.Ensemble;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.datamodel.space.Feature;
import wres.datamodel.time.TimeSeries.Builder;

import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link TimeSeriesOfEnsembleUpscaler}
 * 
 * @author James Brown
 */

public class TimeSeriesOfEnsembleUpscalerTest
{
    private static final String VARIABLE_NAME = "Fruit";
    private static final Feature FEATURE_NAME = Feature.of(
            MessageUtilities.getGeometry( "Tropics" ) );
    private static final String UNIT = "kg/h";

    /**
     * Upscaler instance to test.
     */
    private TimeSeriesUpscaler<Ensemble> upscaler;

    @Before
    public void runBeforeEachTest()
    {
        upscaler = TimeSeriesOfEnsembleUpscaler.of();
    }

    @Test
    public void testUpscaleForecastsCreatesTwoUpscaledForecasts()
    {
        // Eleven event times, PT6H apart
        Instant first = Instant.parse( "1985-01-01T18:00:00Z" );
        Instant second = Instant.parse( "1985-01-02T00:00:00Z" );
        Instant third = Instant.parse( "1985-01-02T06:00:00Z" );
        Instant fourth = Instant.parse( "1985-01-02T12:00:00Z" );

        // Eleven events
        Event<Ensemble> one = Event.of( first, Ensemble.of( 5, 6, 7, 8 ) );
        Event<Ensemble> two = Event.of( second, Ensemble.of( 9, 10, 11, 12 ) );
        Event<Ensemble> three = Event.of( third, Ensemble.of( 13, 14, 15, 16 ) );
        Event<Ensemble> four = Event.of( fourth, Ensemble.of( 17, 18, 19, 20 ) );

        // Time scale of the event values: instantaneous
        TimeScaleOuter existingScale = TimeScaleOuter.of();

        // Forecast reference time
        Instant referenceTime = Instant.parse( "1985-01-01T12:00:00Z" );

        // Time-series to upscale
        TimeSeriesMetadata existingMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( referenceTime,
                                                          existingScale );
        TimeSeries<Ensemble> forecast = new Builder<Ensemble>().addEvent( one )
                                                               .addEvent( two )
                                                               .addEvent( three )
                                                               .addEvent( four )
                                                               .setMetadata( existingMetadata )
                                                               .build();

        // Upscaled forecasts must end at these times
        SortedSet<Instant> endsAt = new TreeSet<>();
        endsAt.add( second );
        endsAt.add( fourth );

        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of( Duration.ofHours( 12 ), TimeScaleFunction.MEAN );

        TimeSeries<Ensemble> actualForecast = this.upscaler.upscale( forecast, desiredTimeScale, endsAt, UNIT )
                                                           .getTimeSeries();

        // Create the expected series with the desired time scale
        Ensemble expectedOne = Ensemble.of( 7, 8, 9, 10 );
        Ensemble expectedTwo = Ensemble.of( 15, 16, 17, 18 );
        TimeSeriesMetadata expectedMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( referenceTime,
                                                          desiredTimeScale );
        TimeSeries<Ensemble> expectedForecast =
                new Builder<Ensemble>().addEvent( Event.of( second, expectedOne ) )
                                       .addEvent( Event.of( fourth, expectedTwo ) )
                                       .setMetadata( expectedMetadata )
                                       .build();

        assertEquals( expectedForecast, actualForecast );
    }

    @Test
    public void testUpscaleVolumetricFlowForecastsToVolumeCreatesOneUpscaledForecast()
    {
        // Six event times, PT1H apart
        Instant first = Instant.parse( "2079-12-03T00:00:00Z" );
        Instant second = Instant.parse( "2079-12-03T01:00:00Z" );
        Instant third = Instant.parse( "2079-12-03T02:00:00Z" );
        Instant fourth = Instant.parse( "2079-12-03T04:00:00Z" );
        Instant fifth = Instant.parse( "2079-12-03T05:00:00Z" );
        Instant sixth = Instant.parse( "2079-12-03T06:00:00Z" );

        // Six events
        Event<Ensemble> one = Event.of( first, Ensemble.of( 12.0, 9.0 ) );
        Event<Ensemble> two = Event.of( second, Ensemble.of( 15.0, 7.5 ) );
        Event<Ensemble> three = Event.of( third, Ensemble.of( 3.0, 8.5 ) );
        Event<Ensemble> four = Event.of( fourth, Ensemble.of( 22.0, 6.9 ) );
        Event<Ensemble> five = Event.of( fifth, Ensemble.of( 11.0, 12.0 ) );
        Event<Ensemble> six = Event.of( sixth, Ensemble.of( 25.0, 21.0 ) );

        // Forecast reference time
        Instant referenceTime = Instant.parse( "1985-01-01T12:00:00Z" );

        // Time scale of the event values: instantaneous
        TimeScaleOuter existingScale = TimeScaleOuter.of( Duration.ofMinutes( 1 ), TimeScaleFunction.MEAN );
        TimeSeriesMetadata existingMetadata =
                TimeSeriesOfEnsembleUpscalerTest.getBoilerplateMetadataWithT0AndTimeScale( referenceTime,
                                                                                           existingScale )
                                                .toBuilder()
                                                .setUnit( "m3/s" )
                                                .build();

        // Time-series to upscale
        TimeSeries<Ensemble> timeSeries = new Builder<Ensemble>().addEvent( one )
                                                                 .addEvent( two )
                                                                 .addEvent( three )
                                                                 .addEvent( four )
                                                                 .addEvent( five )
                                                                 .addEvent( six )
                                                                 .setMetadata( existingMetadata )
                                                                 .build();

        // Where the upscaled values should end (e.g., forecast valid times)
        SortedSet<Instant> endsAt = new TreeSet<>();
        endsAt.add( third );
        endsAt.add( fourth );
        endsAt.add( sixth );

        // The desired scale: mean over PT2H
        TimeScaleOuter desiredTimeScale = TimeScaleOuter.of( Duration.ofHours( 2 ), TimeScaleFunction.TOTAL );

        TimeSeries<Ensemble> actual = this.upscaler.upscale( timeSeries,
                                                             desiredTimeScale,
                                                             endsAt,
                                                             "m3" )
                                                   .getTimeSeries();

        // Create the expected series with the desired time scale
        TimeSeriesMetadata expectedMetadata =
                TimeSeriesOfEnsembleUpscalerTest.getBoilerplateMetadataWithT0AndTimeScale( referenceTime,
                                                                                           desiredTimeScale )
                                                .toBuilder()
                                                .setUnit( "m3" )
                                                .build();

        TimeSeries<Ensemble> expected =
                new Builder<Ensemble>().addEvent( Event.of( third, Ensemble.of( 64800.0, 57600.0 ) ) )
                                       .addEvent( Event.of( sixth, Ensemble.of( 129600.0, 118800.0 ) ) )
                                       .setMetadata( expectedMetadata )
                                       .build();

        assertEquals( expected, actual );
    }


    private static TimeSeriesMetadata getBoilerplateMetadataWithT0AndTimeScale( Instant t0,
                                                                                TimeScaleOuter timeScale )
    {
        return TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, t0 ),
                                      timeScale,
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

}
