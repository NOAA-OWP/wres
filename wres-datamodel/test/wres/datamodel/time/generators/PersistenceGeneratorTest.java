package wres.datamodel.time.generators;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import wres.datamodel.messages.MessageFactory;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureKey;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.statistics.generated.TimeScale;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.datamodel.time.TimeSeries.Builder;

/**
 * Tests the {@link PersistenceGenerator}.
 * 
 * @author James Brown
 */

public class PersistenceGeneratorTest
{

    private static final String STREAMFLOW = "STREAMFLOW";
    private static final String CMS = "CMS";
    private static final String DISCHARGE = "DISCHARGE";
    private static final FeatureKey FAKE2 = FeatureKey.of(
                                                           MessageFactory.getGeometry( "FAKE2" ) );

    // Times used    
    private static final Instant T2551_03_20T12_00_00Z = Instant.parse( "2551-03-20T12:00:00Z" );
    private static final Instant T2551_03_20T11_00_00Z = Instant.parse( "2551-03-20T11:00:00Z" );
    private static final Instant T2551_03_20T10_00_00Z = Instant.parse( "2551-03-20T10:00:00Z" );
    private static final Instant T2551_03_20T09_00_00Z = Instant.parse( "2551-03-20T09:00:00Z" );
    private static final Instant T2551_03_20T08_00_00Z = Instant.parse( "2551-03-20T08:00:00Z" );
    private static final Instant T2551_03_20T07_00_00Z = Instant.parse( "2551-03-20T07:00:00Z" );
    private static final Instant T2551_03_20T06_00_00Z = Instant.parse( "2551-03-20T06:00:00Z" );
    private static final Instant T2551_03_20T05_00_00Z = Instant.parse( "2551-03-20T05:00:00Z" );
    private static final Instant T2551_03_20T04_00_00Z = Instant.parse( "2551-03-20T04:00:00Z" );
    private static final Instant T2551_03_20T03_00_00Z = Instant.parse( "2551-03-20T03:00:00Z" );
    private static final Instant T2551_03_20T02_00_00Z = Instant.parse( "2551-03-20T02:00:00Z" );
    private static final Instant T2551_03_20T01_00_00Z = Instant.parse( "2551-03-20T01:00:00Z" );
    private static final Instant T2551_03_20T00_00_00Z = Instant.parse( "2551-03-20T00:00:00Z" );
    private static final Instant T2551_03_19T23_00_00Z = Instant.parse( "2551-03-19T23:00:00Z" );
    private static final Instant T2551_03_19T22_00_00Z = Instant.parse( "2551-03-19T22:00:00Z" );
    private static final Instant T2551_03_19T21_00_00Z = Instant.parse( "2551-03-19T21:00:00Z" );
    private static final Instant T2551_03_19T20_00_00Z = Instant.parse( "2551-03-19T20:00:00Z" );
    private static final Instant T2551_03_19T19_00_00Z = Instant.parse( "2551-03-19T19:00:00Z" );
    private static final Instant T2551_03_19T18_00_00Z = Instant.parse( "2551-03-19T18:00:00Z" );
    private static final Instant T2551_03_19T17_00_00Z = Instant.parse( "2551-03-19T17:00:00Z" );
    private static final Instant T2551_03_19T16_00_00Z = Instant.parse( "2551-03-19T16:00:00Z" );
    private static final Instant T2551_03_19T15_00_00Z = Instant.parse( "2551-03-19T15:00:00Z" );
    private static final Instant T2551_03_19T14_00_00Z = Instant.parse( "2551-03-19T14:00:00Z" );
    private static final Instant T2551_03_19T13_00_00Z = Instant.parse( "2551-03-19T13:00:00Z" );
    private static final Instant T2551_03_19T12_00_00Z = Instant.parse( "2551-03-19T12:00:00Z" );
    private static final Instant T2551_03_19T11_00_00Z = Instant.parse( "2551-03-19T11:00:00Z" );
    private static final Instant T2551_03_19T10_00_00Z = Instant.parse( "2551-03-19T10:00:00Z" );
    private static final Instant T2551_03_19T09_00_00Z = Instant.parse( "2551-03-19T09:00:00Z" );
    private static final Instant T2551_03_19T08_00_00Z = Instant.parse( "2551-03-19T08:00:00Z" );
    private static final Instant T2551_03_19T07_00_00Z = Instant.parse( "2551-03-19T07:00:00Z" );
    private static final Instant T2551_03_19T06_00_00Z = Instant.parse( "2551-03-19T06:00:00Z" );
    private static final Instant T2551_03_19T05_00_00Z = Instant.parse( "2551-03-19T05:00:00Z" );
    private static final Instant T2551_03_19T04_00_00Z = Instant.parse( "2551-03-19T04:00:00Z" );
    private static final Instant T2551_03_19T03_00_00Z = Instant.parse( "2551-03-19T03:00:00Z" );
    private static final Instant T2551_03_19T02_00_00Z = Instant.parse( "2551-03-19T02:00:00Z" );
    private static final Instant T2551_03_19T01_00_00Z = Instant.parse( "2551-03-19T01:00:00Z" );
    private static final Instant T2551_03_19T00_00_00Z = Instant.parse( "2551-03-19T00:00:00Z" );
    private static final Instant T2551_03_18T23_00_00Z = Instant.parse( "2551-03-18T23:00:00Z" );
    private static final Instant T2551_03_18T22_00_00Z = Instant.parse( "2551-03-18T22:00:00Z" );
    private static final Instant T2551_03_18T21_00_00Z = Instant.parse( "2551-03-18T21:00:00Z" );
    private static final Instant T2551_03_18T20_00_00Z = Instant.parse( "2551-03-18T20:00:00Z" );
    private static final Instant T2551_03_18T19_00_00Z = Instant.parse( "2551-03-18T19:00:00Z" );
    private static final Instant T2551_03_18T18_00_00Z = Instant.parse( "2551-03-18T18:00:00Z" );
    private static final Instant T2551_03_18T17_00_00Z = Instant.parse( "2551-03-18T17:00:00Z" );
    private static final Instant T2551_03_18T16_00_00Z = Instant.parse( "2551-03-18T16:00:00Z" );
    private static final Instant T2551_03_18T15_00_00Z = Instant.parse( "2551-03-18T15:00:00Z" );
    private static final Instant T2551_03_18T14_00_00Z = Instant.parse( "2551-03-18T14:00:00Z" );
    private static final Instant T2551_03_18T13_00_00Z = Instant.parse( "2551-03-18T13:00:00Z" );
    private static final Instant T2551_03_18T12_00_00Z = Instant.parse( "2551-03-18T12:00:00Z" );
    private static final Instant T2551_03_18T11_00_00Z = Instant.parse( "2551-03-18T11:00:00Z" );
    private static final Instant T2551_03_18T10_00_00Z = Instant.parse( "2551-03-18T10:00:00Z" );
    private static final Instant T2551_03_18T09_00_00Z = Instant.parse( "2551-03-18T09:00:00Z" );
    private static final Instant T2551_03_18T08_00_00Z = Instant.parse( "2551-03-18T08:00:00Z" );
    private static final Instant T2551_03_18T07_00_00Z = Instant.parse( "2551-03-18T07:00:00Z" );
    private static final Instant T2551_03_18T06_00_00Z = Instant.parse( "2551-03-18T06:00:00Z" );
    private static final Instant T2551_03_18T05_00_00Z = Instant.parse( "2551-03-18T05:00:00Z" );
    private static final Instant T2551_03_18T04_00_00Z = Instant.parse( "2551-03-18T04:00:00Z" );
    private static final Instant T2551_03_18T03_00_00Z = Instant.parse( "2551-03-18T03:00:00Z" );
    private static final Instant T2551_03_18T02_00_00Z = Instant.parse( "2551-03-18T02:00:00Z" );
    private static final Instant T2551_03_18T01_00_00Z = Instant.parse( "2551-03-18T01:00:00Z" );
    private static final Instant T2551_03_18T00_00_00Z = Instant.parse( "2551-03-18T00:00:00Z" );
    private static final Instant T2551_03_17T23_00_00Z = Instant.parse( "2551-03-17T23:00:00Z" );
    private static final Instant T2551_03_17T22_00_00Z = Instant.parse( "2551-03-17T22:00:00Z" );
    private static final Instant T2551_03_17T21_00_00Z = Instant.parse( "2551-03-17T21:00:00Z" );
    private static final Instant T2551_03_17T20_00_00Z = Instant.parse( "2551-03-17T20:00:00Z" );
    private static final Instant T2551_03_17T19_00_00Z = Instant.parse( "2551-03-17T19:00:00Z" );
    private static final Instant T2551_03_17T18_00_00Z = Instant.parse( "2551-03-17T18:00:00Z" );
    private static final Instant T2551_03_17T17_00_00Z = Instant.parse( "2551-03-17T17:00:00Z" );
    private static final Instant T2551_03_17T16_00_00Z = Instant.parse( "2551-03-17T16:00:00Z" );
    private static final Instant T2551_03_17T15_00_00Z = Instant.parse( "2551-03-17T15:00:00Z" );
    private static final Instant T2551_03_17T14_00_00Z = Instant.parse( "2551-03-17T14:00:00Z" );
    private static final Instant T2551_03_17T13_00_00Z = Instant.parse( "2551-03-17T13:00:00Z" );
    private static final Instant T2551_03_17T12_00_00Z = Instant.parse( "2551-03-17T12:00:00Z" );
    private static final Instant T2551_03_17T11_00_00Z = Instant.parse( "2551-03-17T11:00:00Z" );
    private static final Instant T2551_03_17T10_00_00Z = Instant.parse( "2551-03-17T10:00:00Z" );
    private static final Instant T2551_03_17T09_00_00Z = Instant.parse( "2551-03-17T09:00:00Z" );
    private static final Instant T2551_03_17T08_00_00Z = Instant.parse( "2551-03-17T08:00:00Z" );
    private static final Instant T2551_03_17T07_00_00Z = Instant.parse( "2551-03-17T07:00:00Z" );
    private static final Instant T2551_03_17T06_00_00Z = Instant.parse( "2551-03-17T06:00:00Z" );
    private static final Instant T2551_03_17T05_00_00Z = Instant.parse( "2551-03-17T05:00:00Z" );
    private static final Instant T2551_03_17T04_00_00Z = Instant.parse( "2551-03-17T04:00:00Z" );
    private static final Instant T2551_03_17T03_00_00Z = Instant.parse( "2551-03-17T03:00:00Z" );
    private static final Instant T2551_03_17T02_00_00Z = Instant.parse( "2551-03-17T02:00:00Z" );
    private static final Instant T2551_03_17T01_00_00Z = Instant.parse( "2551-03-17T01:00:00Z" );
    private static final Instant T2551_03_17T00_00_00Z = Instant.parse( "2551-03-17T00:00:00Z" );

    /** Desired time scale. */
    private static final TimeScaleOuter TIMESCALE = TimeScaleOuter.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );

    /** Monthday time scale. */
    private static final TimeScaleOuter MONTHDAY_TIMESCALE = TimeScaleOuter.of( TimeScale.newBuilder()
                                                                                         .setFunction( TimeScaleFunction.MEAN )
                                                                                         .setStartDay( 1 )
                                                                                         .setStartMonth( 4 )
                                                                                         .setEndDay( 2 )
                                                                                         .setEndMonth( 4 )
                                                                                         .build() );


    /** Forecast: 25510317T12_FAKE2_forecast.xml */
    private TimeSeries<Double> forecastOne;

    /** Forecast: 25510318T00_FAKE2_forecast.xml */
    private TimeSeries<Double> forecastTwo;

    /** Forecast: 25510318T12_FAKE2_forecast.xml */
    private TimeSeries<Double> forecastThree;

    /** Forecast: 25510319T00_FAKE2_forecast.xml */
    private TimeSeries<Double> forecastFour;

    /** A persistence generator. */
    private PersistenceGenerator<Double> generator;

    @Before
    public void runBeforeEachTest()
    {

        // Observations: 25510317T00_FAKE2_observations.xml
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                     T2551_03_17T12_00_00Z ),
                                                             TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                                TimeScaleFunction.MEAN ),
                                                             DISCHARGE,
                                                             FAKE2,
                                                             CMS );
        TimeSeries<Double> observations =
                new Builder<Double>().addEvent( Event.of( T2551_03_17T00_00_00Z, 313.0 ) )
                                     .addEvent( Event.of( T2551_03_17T01_00_00Z, 317.0 ) )
                                     .addEvent( Event.of( T2551_03_17T02_00_00Z, 331.0 ) )
                                     .addEvent( Event.of( T2551_03_17T03_00_00Z, 347.0 ) )
                                     .addEvent( Event.of( T2551_03_17T04_00_00Z, 349.0 ) )
                                     .addEvent( Event.of( T2551_03_17T05_00_00Z, 353.0 ) )
                                     .addEvent( Event.of( T2551_03_17T06_00_00Z, 359.0 ) )
                                     .addEvent( Event.of( T2551_03_17T07_00_00Z, 367.0 ) )
                                     .addEvent( Event.of( T2551_03_17T08_00_00Z, 373.0 ) )
                                     .addEvent( Event.of( T2551_03_17T09_00_00Z, 379.0 ) )
                                     .addEvent( Event.of( T2551_03_17T10_00_00Z, 383.0 ) )
                                     .addEvent( Event.of( T2551_03_17T11_00_00Z, 389.0 ) )
                                     .addEvent( Event.of( T2551_03_17T12_00_00Z, 397.0 ) )
                                     .addEvent( Event.of( T2551_03_17T13_00_00Z, 401.0 ) )
                                     .addEvent( Event.of( T2551_03_17T14_00_00Z, 409.0 ) )
                                     .addEvent( Event.of( T2551_03_17T15_00_00Z, 419.0 ) )
                                     .addEvent( Event.of( T2551_03_17T16_00_00Z, 421.0 ) )
                                     .addEvent( Event.of( T2551_03_17T17_00_00Z, 431.0 ) )
                                     .addEvent( Event.of( T2551_03_17T18_00_00Z, 433.0 ) )
                                     .addEvent( Event.of( T2551_03_17T19_00_00Z, 439.0 ) )
                                     .addEvent( Event.of( T2551_03_17T20_00_00Z, 443.0 ) )
                                     .addEvent( Event.of( T2551_03_17T21_00_00Z, 449.0 ) )
                                     .addEvent( Event.of( T2551_03_17T22_00_00Z, 457.0 ) )
                                     .addEvent( Event.of( T2551_03_17T23_00_00Z, 461.0 ) )
                                     .addEvent( Event.of( T2551_03_18T00_00_00Z, 463.0 ) )
                                     .addEvent( Event.of( T2551_03_18T01_00_00Z, 467.0 ) )
                                     .addEvent( Event.of( T2551_03_18T02_00_00Z, 479.0 ) )
                                     .addEvent( Event.of( T2551_03_18T03_00_00Z, 487.0 ) )
                                     .addEvent( Event.of( T2551_03_18T04_00_00Z, 491.0 ) )
                                     .addEvent( Event.of( T2551_03_18T05_00_00Z, 499.0 ) )
                                     .addEvent( Event.of( T2551_03_18T06_00_00Z, 503.0 ) )
                                     .addEvent( Event.of( T2551_03_18T07_00_00Z, 509.0 ) )
                                     .addEvent( Event.of( T2551_03_18T08_00_00Z, 521.0 ) )
                                     .addEvent( Event.of( T2551_03_18T09_00_00Z, 523.0 ) )
                                     .addEvent( Event.of( T2551_03_18T10_00_00Z, 541.0 ) )
                                     .addEvent( Event.of( T2551_03_18T11_00_00Z, 547.0 ) )
                                     .addEvent( Event.of( T2551_03_18T12_00_00Z, 557.0 ) )
                                     .addEvent( Event.of( T2551_03_18T13_00_00Z, 563.0 ) )
                                     .addEvent( Event.of( T2551_03_18T14_00_00Z, 569.0 ) )
                                     .addEvent( Event.of( T2551_03_18T15_00_00Z, 571.0 ) )
                                     .addEvent( Event.of( T2551_03_18T16_00_00Z, 577.0 ) )
                                     .addEvent( Event.of( T2551_03_18T17_00_00Z, 587.0 ) )
                                     .addEvent( Event.of( T2551_03_18T18_00_00Z, 593.0 ) )
                                     .addEvent( Event.of( T2551_03_18T19_00_00Z, 599.0 ) )
                                     .addEvent( Event.of( T2551_03_18T20_00_00Z, 601.0 ) )
                                     .addEvent( Event.of( T2551_03_18T21_00_00Z, 607.0 ) )
                                     .addEvent( Event.of( T2551_03_18T22_00_00Z, 613.0 ) )
                                     .addEvent( Event.of( T2551_03_18T23_00_00Z, 617.0 ) )
                                     .addEvent( Event.of( T2551_03_19T00_00_00Z, 619.0 ) )
                                     .addEvent( Event.of( T2551_03_19T01_00_00Z, 631.0 ) )
                                     .addEvent( Event.of( T2551_03_19T02_00_00Z, 641.0 ) )
                                     .addEvent( Event.of( T2551_03_19T03_00_00Z, 643.0 ) )
                                     .addEvent( Event.of( T2551_03_19T04_00_00Z, 647.0 ) )
                                     .addEvent( Event.of( T2551_03_19T05_00_00Z, 653.0 ) )
                                     .addEvent( Event.of( T2551_03_19T06_00_00Z, 659.0 ) )
                                     .addEvent( Event.of( T2551_03_19T07_00_00Z, 661.0 ) )
                                     .addEvent( Event.of( T2551_03_19T08_00_00Z, 673.0 ) )
                                     .addEvent( Event.of( T2551_03_19T09_00_00Z, 677.0 ) )
                                     .addEvent( Event.of( T2551_03_19T10_00_00Z, 683.0 ) )
                                     .addEvent( Event.of( T2551_03_19T11_00_00Z, 691.0 ) )
                                     .addEvent( Event.of( T2551_03_19T12_00_00Z, 701.0 ) )
                                     .addEvent( Event.of( T2551_03_19T13_00_00Z, 709.0 ) )
                                     .addEvent( Event.of( T2551_03_19T14_00_00Z, 719.0 ) )
                                     .addEvent( Event.of( T2551_03_19T15_00_00Z, 727.0 ) )
                                     .addEvent( Event.of( T2551_03_19T16_00_00Z, 733.0 ) )
                                     .addEvent( Event.of( T2551_03_19T17_00_00Z, 739.0 ) )
                                     .addEvent( Event.of( T2551_03_19T18_00_00Z, 743.0 ) )
                                     .addEvent( Event.of( T2551_03_19T19_00_00Z, 751.0 ) )
                                     .addEvent( Event.of( T2551_03_19T20_00_00Z, 757.0 ) )
                                     .addEvent( Event.of( T2551_03_19T21_00_00Z, 761.0 ) )
                                     .addEvent( Event.of( T2551_03_19T22_00_00Z, 769.0 ) )
                                     .addEvent( Event.of( T2551_03_19T23_00_00Z, 773.0 ) )
                                     .addEvent( Event.of( T2551_03_20T00_00_00Z, 787.0 ) )
                                     .addEvent( Event.of( T2551_03_20T01_00_00Z, 797.0 ) )
                                     .addEvent( Event.of( T2551_03_20T02_00_00Z, 809.0 ) )
                                     .addEvent( Event.of( T2551_03_20T03_00_00Z, 811.0 ) )
                                     .addEvent( Event.of( T2551_03_20T04_00_00Z, 821.0 ) )
                                     .addEvent( Event.of( T2551_03_20T05_00_00Z, 823.0 ) )
                                     .addEvent( Event.of( T2551_03_20T06_00_00Z, 827.0 ) )
                                     .addEvent( Event.of( T2551_03_20T07_00_00Z, 829.0 ) )
                                     .addEvent( Event.of( T2551_03_20T08_00_00Z, 839.0 ) )
                                     .addEvent( Event.of( T2551_03_20T09_00_00Z, 853.0 ) )
                                     .addEvent( Event.of( T2551_03_20T10_00_00Z, 857.0 ) )
                                     .addEvent( Event.of( T2551_03_20T11_00_00Z, 859.0 ) )
                                     .addEvent( Event.of( T2551_03_20T12_00_00Z, 863.0 ) )
                                     .setMetadata( metadata )
                                     .build();

        // Forecast time scale
        TimeScaleOuter existingTimeScale = TimeScaleOuter.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );
        TimeSeriesMetadata metadataOne = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                        T2551_03_17T12_00_00Z ),
                                                                existingTimeScale,
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );
        // Forecast: 25510317T12_FAKE2_forecast.xml
        this.forecastOne =
                new Builder<Double>().addEvent( Event.of( T2551_03_17T15_00_00Z, 73.0 ) )
                                     .addEvent( Event.of( T2551_03_17T18_00_00Z, 79.0 ) )
                                     .addEvent( Event.of( T2551_03_17T21_00_00Z, 83.0 ) )
                                     .addEvent( Event.of( T2551_03_18T00_00_00Z, 89.0 ) )
                                     .addEvent( Event.of( T2551_03_18T03_00_00Z, 97.0 ) )
                                     .addEvent( Event.of( T2551_03_18T06_00_00Z, 101.0 ) )
                                     .addEvent( Event.of( T2551_03_18T09_00_00Z, 103.0 ) )
                                     .addEvent( Event.of( T2551_03_18T12_00_00Z, 107.0 ) )
                                     .addEvent( Event.of( T2551_03_18T15_00_00Z, 109.0 ) )
                                     .addEvent( Event.of( T2551_03_18T18_00_00Z, 113.0 ) )
                                     .addEvent( Event.of( T2551_03_18T21_00_00Z, 127.0 ) )
                                     .setMetadata( metadataOne )
                                     .build();

        TimeSeriesMetadata metadataTwo = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                        T2551_03_18T00_00_00Z ),
                                                                existingTimeScale,
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );
        // Forecast: 25510318T00_FAKE2_forecast.xml
        this.forecastTwo =
                new Builder<Double>().addEvent( Event.of( T2551_03_18T03_00_00Z, 131.0 ) )
                                     .addEvent( Event.of( T2551_03_18T06_00_00Z, 137.0 ) )
                                     .addEvent( Event.of( T2551_03_18T09_00_00Z, 139.0 ) )
                                     .addEvent( Event.of( T2551_03_18T12_00_00Z, 149.0 ) )
                                     .addEvent( Event.of( T2551_03_18T15_00_00Z, 151.0 ) )
                                     .addEvent( Event.of( T2551_03_18T18_00_00Z, 157.0 ) )
                                     .addEvent( Event.of( T2551_03_18T21_00_00Z, 163.0 ) )
                                     .addEvent( Event.of( T2551_03_19T00_00_00Z, 167.0 ) )
                                     .addEvent( Event.of( T2551_03_19T03_00_00Z, 173.0 ) )
                                     .addEvent( Event.of( T2551_03_19T06_00_00Z, 179.0 ) )
                                     .addEvent( Event.of( T2551_03_19T09_00_00Z, 181.0 ) )
                                     .setMetadata( metadataTwo )
                                     .build();

        TimeSeriesMetadata metadataThree = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                          T2551_03_18T12_00_00Z ),
                                                                  existingTimeScale,
                                                                  STREAMFLOW,
                                                                  FAKE2,
                                                                  CMS );
        // Forecast: 25510318T12_FAKE2_forecast.xml
        this.forecastThree =
                new Builder<Double>().addEvent( Event.of( T2551_03_18T15_00_00Z, 191.0 ) )
                                     .addEvent( Event.of( T2551_03_18T18_00_00Z, 193.0 ) )
                                     .addEvent( Event.of( T2551_03_18T21_00_00Z, 197.0 ) )
                                     .addEvent( Event.of( T2551_03_19T00_00_00Z, 199.0 ) )
                                     .addEvent( Event.of( T2551_03_19T03_00_00Z, 211.0 ) )
                                     .addEvent( Event.of( T2551_03_19T06_00_00Z, 223.0 ) )
                                     .addEvent( Event.of( T2551_03_19T09_00_00Z, 227.0 ) )
                                     .addEvent( Event.of( T2551_03_19T12_00_00Z, 229.0 ) )
                                     .addEvent( Event.of( T2551_03_19T15_00_00Z, 233.0 ) )
                                     .addEvent( Event.of( T2551_03_19T18_00_00Z, 239.0 ) )
                                     .addEvent( Event.of( T2551_03_19T21_00_00Z, 241.0 ) )
                                     .setMetadata( metadataThree )
                                     .build();

        TimeSeriesMetadata metadataFour = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                         T2551_03_19T00_00_00Z ),
                                                                 existingTimeScale,
                                                                 STREAMFLOW,
                                                                 FAKE2,
                                                                 CMS );
        // Forecast: 25510319T00_FAKE2_forecast.xml
        this.forecastFour =
                new Builder<Double>().addEvent( Event.of( T2551_03_19T03_00_00Z, 251.0 ) )
                                     .addEvent( Event.of( T2551_03_19T06_00_00Z, 257.0 ) )
                                     .addEvent( Event.of( T2551_03_19T09_00_00Z, 263.0 ) )
                                     .addEvent( Event.of( T2551_03_19T12_00_00Z, 269.0 ) )
                                     .addEvent( Event.of( T2551_03_19T15_00_00Z, 271.0 ) )
                                     .addEvent( Event.of( T2551_03_19T18_00_00Z, 277.0 ) )
                                     .addEvent( Event.of( T2551_03_19T21_00_00Z, 281.0 ) )
                                     .addEvent( Event.of( T2551_03_20T00_00_00Z, 283.0 ) )
                                     .addEvent( Event.of( T2551_03_20T03_00_00Z, 293.0 ) )
                                     .addEvent( Event.of( T2551_03_20T06_00_00Z, 307.0 ) )
                                     .addEvent( Event.of( T2551_03_20T09_00_00Z, 311.0 ) )
                                     .setMetadata( metadataFour )
                                     .build();

        this.generator = PersistenceGenerator.of( () -> Stream.of( observations ),
                                                  TimeSeriesOfDoubleUpscaler.of(),
                                                  Double::isFinite,
                                                  CMS );
    }

    /**
     * Tests the creation of a persistence forecast that corresponds to 25510317T12_FAKE2_forecast.xml from system test 
     * scenario504 as of commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    public void testApplyReturnsOnePersistenceForecastWithElevenValuesForForecastOneFromSystemTestScenario504()
    {

        // Create the persistence forecast using 25510317T12_FAKE2_forecast.xml
        TimeSeries<Double> actual = this.generator.apply( this.forecastOne );

        // Expected persistence forecast
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                     T2551_03_17T12_00_00Z ),
                                                             TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );
        TimeSeries<Double> expected =
                new Builder<Double>().addEvent( Event.of( T2551_03_17T15_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_17T18_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_17T21_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T00_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T03_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T06_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T09_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T12_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T15_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T18_00_00Z, 383.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T21_00_00Z, 383.6666666666667 ) )
                                     .setMetadata( metadata )
                                     .build();

        assertEquals( expected, actual );
    }

    /**
     * Tests the creation of a persistence forecast that corresponds to 25510318T00_FAKE2_forecast.xml from system test 
     * scenario504 as of commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    public void testApplyReturnsOnePersistenceForecastWithElevenValuesForForecastTwoFromSystemTestScenario504()
    {
        // Create the persistence forecast using 25510318T00_FAKE2_forecast.xml
        TimeSeries<Double> actual = this.generator.apply( this.forecastTwo );

        // Expected persistence forecast
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                     T2551_03_18T00_00_00Z ),
                                                             TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );
        TimeSeries<Double> expected =
                new Builder<Double>().addEvent( Event.of( T2551_03_18T03_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T06_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T09_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T12_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T15_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T18_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_18T21_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_19T00_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_19T03_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_19T06_00_00Z, 455.6666666666667 ) )
                                     .addEvent( Event.of( T2551_03_19T09_00_00Z, 455.6666666666667 ) )
                                     .setMetadata( metadata )
                                     .build();

        assertEquals( expected, actual );
    }

    /**
     * Tests the creation of a persistence forecast that corresponds to 25510318T12_FAKE2_forecast.xml from system test 
     * scenario504 as of commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    public void testApplyReturnsOnePersistenceForecastWithElevenValuesForForecastThreeFromSystemTestScenario504()
    {
        // Create the persistence forecast using 25510318T12_FAKE2_forecast.xml
        TimeSeries<Double> actual = this.generator.apply( this.forecastThree );

        // Expected persistence forecast
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                     T2551_03_18T12_00_00Z ),
                                                             TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );
        TimeSeries<Double> expected =
                new Builder<Double>().addEvent( Event.of( T2551_03_18T15_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_18T18_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_18T21_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_19T00_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_19T03_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_19T06_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_19T09_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_19T12_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_19T15_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_19T18_00_00Z, 537.0 ) )
                                     .addEvent( Event.of( T2551_03_19T21_00_00Z, 537.0 ) )
                                     .setMetadata( metadata )
                                     .build();

        assertEquals( expected, actual );
    }

    /**
     * Tests the creation of a persistence forecast that corresponds to 25510319T00_FAKE2_forecast.xml from system test 
     * scenario504 as of commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    public void testApplyReturnsOnePersistenceForecastWithElevenValuesForForecastFourFromSystemTestScenario504()
    {
        // Create the persistence forecast using 25510319T00_FAKE2_forecast.xml
        TimeSeries<Double> actual = this.generator.apply( this.forecastFour );

        // Expected persistence forecast
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                     T2551_03_19T00_00_00Z ),
                                                             TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );
        TimeSeries<Double> expected =
                new Builder<Double>().addEvent( Event.of( T2551_03_19T03_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_19T06_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_19T09_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_19T12_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_19T15_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_19T18_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_19T21_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_20T00_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_20T03_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_20T06_00_00Z, 612.3333333333334 ) )
                                     .addEvent( Event.of( T2551_03_20T09_00_00Z, 612.3333333333334 ) )
                                     .setMetadata( metadata )
                                     .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testGetForSimulations()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );

        TimeSeries<Double> series = new Builder<Double>().addEvent( Event.of( T2551_03_18T15_00_00Z, 1.0 ) )
                                                         .addEvent( Event.of( T2551_03_18T18_00_00Z, 2.0 ) )
                                                         .addEvent( Event.of( T2551_03_18T21_00_00Z, 3.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T00_00_00Z, 4.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T03_00_00Z, 5.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T06_00_00Z, 6.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T09_00_00Z, 7.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T12_00_00Z, 8.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T15_00_00Z, 9.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T18_00_00Z, 10.0 ) )
                                                         .setMetadata( metadata )
                                                         .build();

        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( series ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( series );

        TimeSeries<Double> expected = new Builder<Double>().addEvent( Event.of( T2551_03_18T18_00_00Z, 1.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T21_00_00Z, 2.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T00_00_00Z, 3.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T03_00_00Z, 4.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T06_00_00Z, 5.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T09_00_00Z, 6.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T12_00_00Z, 7.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T15_00_00Z, 8.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T18_00_00Z, 9.0 ) )
                                                           .setMetadata( metadata )
                                                           .build();

        assertEquals( expected, actual );
    }

    /**
     * Finds the nearest value to use as the persistence value in a simulated time-series whose valid times differ from 
     * those of the source ("observed") series.
     */

    @Test
    public void testApplyToSimulationsWithNoExactMatches()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );

        TimeSeries<Double> series = new Builder<Double>().addEvent( Event.of( T2551_03_18T15_00_00Z, 1.0 ) )
                                                         .addEvent( Event.of( T2551_03_18T18_00_00Z, 2.0 ) )
                                                         .addEvent( Event.of( T2551_03_18T21_00_00Z, 3.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T00_00_00Z, 4.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T03_00_00Z, 5.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T06_00_00Z, 6.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T09_00_00Z, 7.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T12_00_00Z, 8.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T15_00_00Z, 9.0 ) )
                                                         .addEvent( Event.of( T2551_03_19T18_00_00Z, 10.0 ) )
                                                         .setMetadata( metadata )
                                                         .build();

        TimeSeries<Double> anotherSeries = new Builder<Double>().addEvent( Event.of( T2551_03_18T16_00_00Z, 1.0 ) )
                                                                .addEvent( Event.of( T2551_03_18T19_00_00Z, 2.0 ) )
                                                                .addEvent( Event.of( T2551_03_18T22_00_00Z, 3.0 ) )
                                                                .addEvent( Event.of( T2551_03_19T01_00_00Z, 4.0 ) )
                                                                .addEvent( Event.of( T2551_03_19T04_00_00Z, 5.0 ) )
                                                                .addEvent( Event.of( T2551_03_19T07_00_00Z, 6.0 ) )
                                                                .addEvent( Event.of( T2551_03_19T10_00_00Z, 7.0 ) )
                                                                .addEvent( Event.of( T2551_03_19T13_00_00Z, 8.0 ) )
                                                                .addEvent( Event.of( T2551_03_19T16_00_00Z, 9.0 ) )
                                                                .addEvent( Event.of( T2551_03_19T19_00_00Z, 10.0 ) )
                                                                .setMetadata( metadata )
                                                                .build();


        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( anotherSeries ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( series );

        TimeSeries<Double> expected = new Builder<Double>().addEvent( Event.of( T2551_03_18T18_00_00Z, 1.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T21_00_00Z, 2.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T00_00_00Z, 3.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T03_00_00Z, 4.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T06_00_00Z, 5.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T09_00_00Z, 6.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T12_00_00Z, 7.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T15_00_00Z, 8.0 ) )
                                                           .addEvent( Event.of( T2551_03_19T18_00_00Z, 9.0 ) )
                                                           .setMetadata( metadata )
                                                           .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyToForecastsWithoutUpscaling()
    {
        TimeSeriesMetadata forecastMetadata = TimeSeriesMetadata.of( Map.of( ReferenceTimeType.UNKNOWN,
                                                                             T2551_03_18T12_00_00Z ),
                                                                     TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                                        TimeScaleFunction.MEAN ),
                                                                     STREAMFLOW,
                                                                     FAKE2,
                                                                     CMS );

        TimeSeries<Double> forecast = new Builder<Double>().addEvent( Event.of( T2551_03_18T13_00_00Z, 1.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T14_00_00Z, 2.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T15_00_00Z, 3.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T16_00_00Z, 4.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T17_00_00Z, 5.0 ) )
                                                           .setMetadata( forecastMetadata )
                                                           .build();

        TimeSeriesMetadata observedMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                     TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                                        TimeScaleFunction.MEAN ),
                                                                     STREAMFLOW,
                                                                     FAKE2,
                                                                     CMS );

        TimeSeries<Double> observed = new Builder<Double>().addEvent( Event.of( T2551_03_18T11_00_00Z, 9.0 ) )
                                                           .setMetadata( observedMetadata )
                                                           .build();

        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( observed ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( forecast );

        TimeSeries<Double> expected = new Builder<Double>().addEvent( Event.of( T2551_03_18T13_00_00Z, 9.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T14_00_00Z, 9.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T15_00_00Z, 9.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T16_00_00Z, 9.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T17_00_00Z, 9.0 ) )
                                                           .setMetadata( forecastMetadata )
                                                           .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyToSimulationsWithUpscaling()
    {
        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                                TimeScaleFunction.MEAN ),
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );

        TimeSeries.Builder<Double> series = new Builder<Double>().addEvent( Event.of( T2551_03_18T12_00_00Z, 1.0 ) )
                                                                 .addEvent( Event.of( T2551_03_18T13_00_00Z, 2.0 ) )
                                                                 .addEvent( Event.of( T2551_03_18T14_00_00Z, 3.0 ) )
                                                                 .addEvent( Event.of( T2551_03_18T15_00_00Z, 4.0 ) )
                                                                 .addEvent( Event.of( T2551_03_18T16_00_00Z, 5.0 ) )
                                                                 .addEvent( Event.of( T2551_03_18T17_00_00Z, 6.0 ) )
                                                                 .addEvent( Event.of( T2551_03_18T18_00_00Z, 7.0 ) )
                                                                 .addEvent( Event.of( T2551_03_18T19_00_00Z, 8.0 ) )
                                                                 .addEvent( Event.of( T2551_03_18T20_00_00Z, 9.0 ) )
                                                                 .setMetadata( metadata );

        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( series.build() ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          CMS );

        TimeSeriesMetadata newMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                TIMESCALE,
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );

        TimeSeries<Double> toGenerate = new Builder<Double>().addEvent( Event.of( T2551_03_18T15_00_00Z, 1.0 ) )
                                                             .addEvent( Event.of( T2551_03_18T18_00_00Z, 1.0 ) )
                                                             .addEvent( Event.of( T2551_03_18T21_00_00Z, 1.0 ) )
                                                             .setMetadata( newMetadata )
                                                             .build();

        TimeSeries<Double> actual = generator.apply( toGenerate );

        TimeSeries<Double> expected = new Builder<Double>().addEvent( Event.of( T2551_03_18T15_00_00Z, 2.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T18_00_00Z, 5.0 ) )
                                                           .addEvent( Event.of( T2551_03_18T21_00_00Z, 8.0 ) )
                                                           .setMetadata( newMetadata )
                                                           .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyToSimulationsWithUpscalingToFixedMonthDaysAndEventBeforeEndMonthDay()
    {
        TimeSeriesMetadata fooMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                TimeScaleOuter.of(),
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );

        TimeSeries<Double> fooSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-04-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-02T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-03T00:00:00Z" ),
                                                                     3.0 ) )
                                                .setMetadata( fooMetadata )
                                                .build();

        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             MONTHDAY_TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );

        // Event month-day falls before the end month-day of the time scale, so the correct lag 1 value is 
        // one year prior, in year 1988
        TimeSeries<Double> fooTemplateSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1989-01-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .setMetadata( metadata )
                                                .build();

        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( fooSeries ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( fooTemplateSeries );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1989-01-01T00:00:00Z" ),
                                                                     1.5 ) )
                                                .setMetadata( metadata )
                                                .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyToSimulationsWithUpscalingToFixedMonthDaysAndEventAfterEndMonthDay()
    {
        TimeSeriesMetadata fooMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                TimeScaleOuter.of(),
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );

        TimeSeries<Double> fooSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-04-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-02T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-03T00:00:00Z" ),
                                                                     3.0 ) )
                                                .setMetadata( fooMetadata )
                                                .build();

        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             MONTHDAY_TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );

        // Event month-day falls after the end month-day of the time scale, so the correct lag 1 value is within the
        // current year, 1988
        TimeSeries<Double> fooTemplateSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-12-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .setMetadata( metadata )
                                                .build();

        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( fooSeries ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( fooTemplateSeries );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-12-01T00:00:00Z" ),
                                                                     1.5 ) )
                                                .setMetadata( metadata )
                                                .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyToSimulationsWithUpscalingToFixedMonthDaysAndEventFallsWithinMonthDayInterval()
    {
        TimeSeriesMetadata fooMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                TimeScaleOuter.of(),
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );

        TimeSeries<Double> fooSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1987-04-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1987-04-02T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1987-04-03T00:00:00Z" ),
                                                                     3.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-02T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-03T00:00:00Z" ),
                                                                     3.0 ) )
                                                .setMetadata( fooMetadata )
                                                .build();

        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             MONTHDAY_TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );

        // Event month-day falls within the time scale interval, so the correct lag 1 value is within the prior
        // year, 1987
        TimeSeries<Double> fooTemplateSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-04-02T00:00:00Z" ),
                                                                     1.0 ) )
                                                .setMetadata( metadata )
                                                .build();

        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( fooSeries ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( fooTemplateSeries );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-04-02T00:00:00Z" ),
                                                                     1.5 ) )
                                                .setMetadata( metadata )
                                                .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyToSimulationsWithUpscalingToFixedMonthDaysAndEventAfterEndMonthDayAndLagTwo()
    {
        TimeSeriesMetadata fooMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                TimeScaleOuter.of(),
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );

        TimeSeries<Double> fooSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1987-04-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1987-04-02T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1987-04-03T00:00:00Z" ),
                                                                     3.0 ) )
                                                .setMetadata( fooMetadata )
                                                .build();

        TimeSeriesMetadata metadata = TimeSeriesMetadata.of( Map.of(),
                                                             MONTHDAY_TIMESCALE,
                                                             STREAMFLOW,
                                                             FAKE2,
                                                             CMS );

        // Event month-day falls after the end month-day of the time scale, so the correct lag 2 value is within the
        // prior year, 1987
        TimeSeries<Double> fooTemplateSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-12-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .setMetadata( metadata )
                                                .build();

        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( fooSeries ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          2,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( fooTemplateSeries );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-12-01T00:00:00Z" ),
                                                                     1.5 ) )
                                                .setMetadata( metadata )
                                                .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyToForecastsWithUpscalingToFixedMonthDays()
    {
        TimeSeriesMetadata fooMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                TimeScaleOuter.of(),
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );

        TimeSeries<Double> fooSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-04-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-02T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-03T00:00:00Z" ),
                                                                     3.0 ) )
                                                .setMetadata( fooMetadata )
                                                .build();

        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, Instant.parse( "1989-01-01T00:00:00Z" ) ),
                                       MONTHDAY_TIMESCALE,
                                       STREAMFLOW,
                                       FAKE2,
                                       CMS );

        TimeSeries<Double> fooTemplateSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1989-01-02T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1989-01-03T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1989-01-04T00:00:00Z" ),
                                                                     3.0 ) )
                                                .setMetadata( metadata )
                                                .build();

        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( fooSeries ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( fooTemplateSeries );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1989-01-02T00:00:00Z" ),
                                                                     1.5 ) )
                                                .addEvent( Event.of( Instant.parse( "1989-01-03T00:00:00Z" ),
                                                                     1.5 ) )
                                                .addEvent( Event.of( Instant.parse( "1989-01-04T00:00:00Z" ),
                                                                     1.5 ) )
                                                .setMetadata( metadata )
                                                .build();

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyToForecastsWithUpscalingToFixedMonthDaysUsingLag3Persistence()
    {
        TimeSeriesMetadata fooMetadata = TimeSeriesMetadata.of( Map.of(),
                                                                TimeScaleOuter.of(),
                                                                STREAMFLOW,
                                                                FAKE2,
                                                                CMS );

        TimeSeries<Double> fooSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1988-04-01T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-02T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1988-04-03T00:00:00Z" ),
                                                                     3.0 ) )
                                                .setMetadata( fooMetadata )
                                                .build();

        TimeScale timeScale = TimeScale.newBuilder()
                                       .setFunction( TimeScaleFunction.MEAN )
                                       .setStartDay( 1 )
                                       .setStartMonth( 4 )
                                       .setEndDay( 2 )
                                       .setEndMonth( 4 )
                                       .build();

        TimeScaleOuter outerTimeScale = TimeScaleOuter.of( timeScale );

        TimeSeriesMetadata metadata =
                TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, Instant.parse( "1991-01-01T00:00:00Z" ) ),
                                       outerTimeScale,
                                       STREAMFLOW,
                                       FAKE2,
                                       CMS );

        TimeSeries<Double> fooTemplateSeries =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1991-01-02T00:00:00Z" ),
                                                                     1.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1991-01-03T00:00:00Z" ),
                                                                     2.0 ) )
                                                .addEvent( Event.of( Instant.parse( "1991-01-04T00:00:00Z" ),
                                                                     3.0 ) )
                                                .setMetadata( metadata )
                                                .build();

        // Lag-3 persistence
        PersistenceGenerator<Double> generator = PersistenceGenerator.of( () -> Stream.of( fooSeries ),
                                                                          TimeSeriesOfDoubleUpscaler.of(),
                                                                          Double::isFinite,
                                                                          3,
                                                                          CMS );

        TimeSeries<Double> actual = generator.apply( fooTemplateSeries );

        TimeSeries<Double> expected =
                new TimeSeries.Builder<Double>().addEvent( Event.of( Instant.parse( "1991-01-02T00:00:00Z" ),
                                                                     1.5 ) )
                                                .addEvent( Event.of( Instant.parse( "1991-01-03T00:00:00Z" ),
                                                                     1.5 ) )
                                                .addEvent( Event.of( Instant.parse( "1991-01-04T00:00:00Z" ),
                                                                     1.5 ) )
                                                .setMetadata( metadata )
                                                .build();

        assertEquals( expected, actual );
    }

}
