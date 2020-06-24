package wres.datamodel.time;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.Ensemble;
import wres.datamodel.FeatureKey;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;

/**
 * Tests the {@link TimeSeriesOfEnsembleUpscaler}
 * 
 * @author james.brown@hydrosolved.com
 */

public class TimeSeriesOfEnsembleUpscalerTest
{
    private static final String VARIABLE_NAME = "Fruit";
    private static final FeatureKey FEATURE_NAME = FeatureKey.of( "Tropics" );
    private static final String UNIT = "kg/h";

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0AndTimeScale( Instant t0,
                                                                                TimeScale timeScale )
    {
        return TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, t0 ),
                                      timeScale,
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }

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
        TimeScale existingScale = TimeScale.of();

        // Forecast reference time
        Instant referenceTime = Instant.parse( "1985-01-01T12:00:00Z" );

        // Time-series to upscale
        TimeSeriesMetadata existingMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( referenceTime,
                                                          existingScale );
        TimeSeries<Ensemble> forecast = new TimeSeriesBuilder<Ensemble>().addEvent( one )
                                                                         .addEvent( two )
                                                                         .addEvent( three )
                                                                         .addEvent( four )
                                                                         .setMetadata( existingMetadata )
                                                                         .build();

        // Upscaled forecasts must end at these times
        Set<Instant> endsAt = new HashSet<>();
        endsAt.add( second );
        endsAt.add( fourth );

        TimeScale desiredTimeScale = TimeScale.of( Duration.ofHours( 12 ), TimeScaleFunction.MEAN );

        TimeSeries<Ensemble> actualForecast = this.upscaler.upscale( forecast, desiredTimeScale, endsAt )
                                                           .getTimeSeries();

        // Create the expected series with the desired time scale
        Ensemble expectedOne = Ensemble.of( 7, 8, 9, 10 );
        Ensemble expectedTwo = Ensemble.of( 15, 16, 17, 18 );
        TimeSeriesMetadata expectedMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( referenceTime,
                                                          desiredTimeScale );
        TimeSeries<Ensemble> expectedForecast =
                new TimeSeriesBuilder<Ensemble>().addEvent( Event.of( second, expectedOne ) )
                                                 .addEvent( Event.of( fourth, expectedTwo ) )
                                                 .setMetadata( expectedMetadata )
                                                 .build();

        assertEquals( expectedForecast, actualForecast );
    }

}
