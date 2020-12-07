package wres.io.pooling;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import wres.datamodel.FeatureTuple;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.FeatureKey;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.scale.TimeScaleOuter.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.io.pooling.PoolSupplier.PoolOfPairsSupplierBuilder;
import wres.io.retrieval.CachingRetriever;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Pool;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;

/**
 * Tests the {@link PoolSupplier}.
 * 
 * @author james.brown@hydrosolved.com
 */

@RunWith( MockitoJUnitRunner.class )
public class PoolSupplierTest
{

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

    private static final String VARIABLE_NAME = "STREAMFLOW";
    private static final FeatureKey FEATURE_NAME = FeatureKey.of( "DRRC2" );
    private static final String UNIT = "CMS";

    private static TimeSeriesMetadata getBoilerplateMetadata()
    {
        return TimeSeriesMetadata.of( Collections.emptyMap(),
                                      TimeScaleOuter.of( Duration.ofHours( 1 ) ),
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
    }


    private static TimeSeriesMetadata getBoilerplateMetadataWithTimeScale( TimeScaleOuter timeScale )
    {
        return TimeSeriesMetadata.of( Collections.emptyMap(),
                                      timeScale,
                                      VARIABLE_NAME,
                                      FEATURE_NAME,
                                      UNIT );
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

    /**
     * Retriever for observations.
     */

    @Mock
    private Supplier<Stream<TimeSeries<Double>>> observationRetriever;

    /**
     * Retriever for forecasts.
     */

    @Mock
    private Supplier<Stream<TimeSeries<Double>>> forecastRetriever;

    /**
     * Observations.
     */

    private TimeSeries<Double> observations;

    /**
     * Forecast: 25510317T12_FAKE2_forecast.xml
     */

    private TimeSeries<Double> forecastOne;

    /**
     * Forecast: 25510318T00_FAKE2_forecast.xml
     */

    private TimeSeries<Double> forecastTwo;

    /**
     * Forecast: 25510318T12_FAKE2_forecast.xml
     */

    private TimeSeries<Double> forecastThree;

    /**
     * Forecast: 25510319T00_FAKE2_forecast.xml
     */

    private TimeSeries<Double> forecastFour;

    /**
     * Desired time scale.
     */

    private TimeScaleOuter desiredTimeScale;

    /**
     * Metadata for common pools.
     */

    private SampleMetadata metadata;

    /**
     * An upscaler.
     */

    private TimeSeriesUpscaler<Double> upscaler;

    /**
     * A pairer.
     */

    private TimeSeriesPairer<Double, Double> pairer;

    @Before
    public void runBeforeEachTest()
    {

        TimeScaleOuter existingTimeScale = TimeScaleOuter.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );

        // Observations: 25510317T00_FAKE2_observations.xml
        TimeSeriesMetadata metadata = getBoilerplateMetadataWithTimeScale( TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                                              TimeScaleFunction.MEAN ) );
        this.observations =
                new TimeSeriesBuilder<Double>().setMetadata( metadata )
                                               .addEvent( Event.of( T2551_03_17T00_00_00Z, 313.0 ) )
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
                                               .build();

        // Forecast: 25510317T12_FAKE2_forecast.xml
        TimeSeriesMetadata forecastOneMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          existingTimeScale );
        this.forecastOne =
                new TimeSeriesBuilder<Double>().setMetadata( forecastOneMetadata )
                                               .addEvent( Event.of( T2551_03_17T15_00_00Z, 73.0 ) )
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
                                               .build();

        // Forecast: 25510318T00_FAKE2_forecast.xml
        TimeSeriesMetadata forecastTwoMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_18T00_00_00Z,
                                                          existingTimeScale );
        this.forecastTwo =
                new TimeSeriesBuilder<Double>().setMetadata( forecastTwoMetadata )
                                               .addEvent( Event.of( T2551_03_18T03_00_00Z, 131.0 ) )
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
                                               .build();

        // Forecast: 25510318T12_FAKE2_forecast.xml
        TimeSeriesMetadata forecastThreeMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_18T12_00_00Z,
                                                          existingTimeScale );
        this.forecastThree =
                new TimeSeriesBuilder<Double>().setMetadata( forecastThreeMetadata )
                                               .addEvent( Event.of( T2551_03_18T15_00_00Z, 191.0 ) )
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
                                               .build();

        // Forecast: 25510319T00_FAKE2_forecast.xml
        TimeSeriesMetadata forecastFourMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_19T00_00_00Z,
                                                          existingTimeScale );
        this.forecastFour =
                new TimeSeriesBuilder<Double>().setMetadata( forecastFourMetadata )
                                               .addEvent( Event.of( T2551_03_19T03_00_00Z, 251.0 ) )
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
                                               .build();

        // Desired time scale
        this.desiredTimeScale = TimeScaleOuter.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );

        // Basic metadata
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "STREAMFLOW" )
                                          .setMeasurementUnit( "CMS" )
                                          .build();

        Pool pool = MessageFactory.parse( new FeatureTuple( FeatureKey.of( "FAKE2" ),
                                                            FeatureKey.of( "FAKE2" ),
                                                            null ),
                                          null,
                                          null,
                                          null,
                                          false );

        this.metadata = SampleMetadata.of( evaluation, pool );

        // Upscaler
        this.upscaler = TimeSeriesOfDoubleUpscaler.of();

        // Pairer
        this.pairer = TimeSeriesPairerByExactTime.of();
    }

    /**
     * Tests the retrieval of expected pairs for the first of eighteen pools from system test scenario505 as of
     * commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    public void testGetReturnsPoolThatContainsSevenPairsInOneSeries()
    {
        // Pool One actual        
        Mockito.when( this.observationRetriever.get() ).thenReturn( Stream.of( this.observations ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() ).thenReturn( Stream.of( this.forecastOne ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierOne = this.forecastRetriever;

        TimeWindowOuter poolOneWindow = TimeWindowOuter.of( T2551_03_17T00_00_00Z, //2551-03-17T00:00:00Z
                                                            T2551_03_17T13_00_00Z, //2551-03-17T13:00:00Z
                                                            Duration.ofHours( 0 ),
                                                            Duration.ofHours( 23 ) );

        SampleMetadata poolOneMetadata = SampleMetadata.of( this.metadata,
                                                            poolOneWindow,
                                                            this.desiredTimeScale );

        Supplier<PoolOfPairs<Double, Double>> poolOneSupplier =
                new PoolOfPairsSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                                .setRight( forcSupplierOne )
                                                                .setLeftUpscaler( this.upscaler )
                                                                .setPairer( this.pairer )
                                                                .setDesiredTimeScale( this.desiredTimeScale )
                                                                .setMetadata( poolOneMetadata )
                                                                .build();

        PoolOfPairs<Double, Double> poolOneActual = poolOneSupplier.get();

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolOneSeries =
                new TimeSeriesBuilder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                             .addEvent( Event.of( T2551_03_17T15_00_00Z,
                                                                                  Pair.of( 409.6666666666667,
                                                                                           73.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_17T18_00_00Z,
                                                                                  Pair.of( 428.3333333333333,
                                                                                           79.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_17T21_00_00Z,
                                                                                  Pair.of( 443.6666666666667,
                                                                                           83.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T00_00_00Z,
                                                                                  Pair.of( 460.3333333333333,
                                                                                           89.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T03_00_00Z,
                                                                                  Pair.of( 477.6666666666667,
                                                                                           97.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T06_00_00Z,
                                                                                  Pair.of( 497.6666666666667,
                                                                                           101.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T09_00_00Z,
                                                                                  Pair.of( 517.6666666666666,
                                                                                           103.0 ) ) )
                                                             .build();

        PoolOfPairs<Double, Double> poolOneExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolOneSeries )
                                                        .setMetadata( poolOneMetadata )
                                                        .build();

        assertEquals( poolOneExpected, poolOneActual );
    }

    /**
     * Tests the retrieval of expected pairs for the first of eighteen pools from system test scenario505 as of
     * commit 725345a6e23df36d3ad2661a068f93563caa07a8 where the pairs are baseline pairs.
     */

    @Test
    public void testGetReturnsPoolThatContainsSevenPairsInOneSeriesForBaselineAndIncludesClimatology()
    {
        // Mock one return of the observed stream, even though it is used for climatology,
        // because we are adding to a CachingRetriever
        Mockito.when( this.observationRetriever.get() ).thenReturn( Stream.of( this.observations ) );

        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        // Using the same source for right and baseline, so allow the stream to be 
        // operated on for each of right and baseline
        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastOne ) )
               .thenReturn( Stream.of( this.forecastOne ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierOne = this.forecastRetriever;

        TimeWindowOuter poolOneWindow = TimeWindowOuter.of( T2551_03_17T00_00_00Z, //2551-03-17T00:00:00Z
                                                            T2551_03_17T13_00_00Z, //2551-03-17T13:00:00Z
                                                            Duration.ofHours( 0 ),
                                                            Duration.ofHours( 23 ) );

        SampleMetadata poolOneMetadata = SampleMetadata.of( this.metadata,
                                                            poolOneWindow,
                                                            this.desiredTimeScale );

        Pool baselinePool = poolOneMetadata.getPool()
                                           .toBuilder()
                                           .setIsBaselinePool( true )
                                           .build();

        SampleMetadata poolOneMetadataBaseline = SampleMetadata.of( poolOneMetadata.getEvaluation(), baselinePool );

        Supplier<PoolOfPairs<Double, Double>> poolOneSupplier =
                new PoolOfPairsSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                                .setRight( forcSupplierOne )
                                                                .setBaseline( forcSupplierOne )
                                                                .setClimatology( obsSupplier, Double::doubleValue )
                                                                .setLeftUpscaler( this.upscaler )
                                                                .setPairer( this.pairer )
                                                                .setDesiredTimeScale( this.desiredTimeScale )
                                                                .setMetadata( poolOneMetadata )
                                                                .setBaselineMetadata( poolOneMetadataBaseline )
                                                                .build();

        // Acquire the pools for the baseline
        PoolOfPairs<Double, Double> poolOneActual = poolOneSupplier.get().getBaselineData();

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolOneSeries =
                new TimeSeriesBuilder<Pair<Double, Double>>().addEvent( Event.of( T2551_03_17T15_00_00Z,
                                                                                  Pair.of( 409.6666666666667,
                                                                                           73.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_17T18_00_00Z,
                                                                                  Pair.of( 428.3333333333333,
                                                                                           79.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_17T21_00_00Z,
                                                                                  Pair.of( 443.6666666666667,
                                                                                           83.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T00_00_00Z,
                                                                                  Pair.of( 460.3333333333333,
                                                                                           89.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T03_00_00Z,
                                                                                  Pair.of( 477.6666666666667,
                                                                                           97.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T06_00_00Z,
                                                                                  Pair.of( 497.6666666666667,
                                                                                           101.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T09_00_00Z,
                                                                                  Pair.of( 517.6666666666666,
                                                                                           103.0 ) ) )
                                                             .setMetadata( poolOneTimeSeriesMetadata )
                                                             .build();

        double[] climatologyArray = this.observations.getEvents()
                                                     .stream()
                                                     .mapToDouble( Event::getValue )
                                                     .toArray();

        VectorOfDoubles expectedClimatology = VectorOfDoubles.of( climatologyArray );

        PoolOfPairs<Double, Double> poolOneExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolOneSeries )
                                                        .setClimatology( expectedClimatology )
                                                        .setMetadata( poolOneMetadataBaseline )
                                                        .build();

        assertEquals( poolOneExpected, poolOneActual );
    }

    /**
     * Tests the retrieval of expected pairs for the eleventh of eighteen pools from system test scenario505 as of
     * commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    public void testGetReturnsPoolThatContainsFourteenPairsInTwoSeries()
    {
        // Pool Eleven actual: NOTE two forecasts
        Mockito.when( this.observationRetriever.get() ).thenReturn( Stream.of( this.observations ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastThree, this.forecastFour ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierEleven = CachingRetriever.of( this.forecastRetriever );

        TimeWindowOuter poolElevenWindow = TimeWindowOuter.of( T2551_03_18T11_00_00Z, //2551-03-18T11:00:00Z
                                                               T2551_03_19T00_00_00Z, //2551-03-19T00:00:00Z
                                                               Duration.ofHours( 0 ),
                                                               Duration.ofHours( 23 ) );

        SampleMetadata poolElevenMetadata = SampleMetadata.of( this.metadata,
                                                               poolElevenWindow,
                                                               this.desiredTimeScale );

        Supplier<PoolOfPairs<Double, Double>> poolElevenSupplier =
                new PoolOfPairsSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                                .setRight( forcSupplierEleven )
                                                                .setLeftUpscaler( this.upscaler )
                                                                .setPairer( this.pairer )
                                                                .setDesiredTimeScale( this.desiredTimeScale )
                                                                .setMetadata( poolElevenMetadata )
                                                                .build();

        PoolOfPairs<Double, Double> poolElevenActual = poolElevenSupplier.get();

        // Pool Eleven expected
        TimeSeriesMetadata poolTimeSeriesMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_18T12_00_00Z,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolElevenOneSeries =
                new TimeSeriesBuilder<Pair<Double, Double>>().addEvent( Event.of( T2551_03_18T15_00_00Z,
                                                                                  Pair.of( 567.6666666666666,
                                                                                           191.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T18_00_00Z,
                                                                                  Pair.of( 585.6666666666666,
                                                                                           193.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_18T21_00_00Z,
                                                                                  Pair.of( 602.3333333333334,
                                                                                           197.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T00_00_00Z,
                                                                                  Pair.of( 616.3333333333334,
                                                                                           199.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T03_00_00Z,
                                                                                  Pair.of( 638.3333333333334,
                                                                                           211.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T06_00_00Z,
                                                                                  Pair.of( 653.0, 223.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T09_00_00Z,
                                                                                  Pair.of( 670.3333333333334,
                                                                                           227.0 ) ) )
                                                             .setMetadata( poolTimeSeriesMetadata )
                                                             .build();

        TimeSeriesMetadata poolTimeSeriesTwoMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_19T00_00_00Z,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolElevenTwoSeries =
                new TimeSeriesBuilder<Pair<Double, Double>>().addEvent( Event.of( T2551_03_19T03_00_00Z,
                                                                                  Pair.of( 638.3333333333334,
                                                                                           251.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T06_00_00Z,
                                                                                  Pair.of( 653.0, 257.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T09_00_00Z,
                                                                                  Pair.of( 670.3333333333334,
                                                                                           263.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T12_00_00Z,
                                                                                  Pair.of( 691.6666666666666,
                                                                                           269.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T15_00_00Z,
                                                                                  Pair.of( 718.3333333333334,
                                                                                           271.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T18_00_00Z,
                                                                                  Pair.of( 738.3333333333334,
                                                                                           277.0 ) ) )
                                                             .addEvent( Event.of( T2551_03_19T21_00_00Z,
                                                                                  Pair.of( 756.3333333333334,
                                                                                           281.0 ) ) )
                                                             .setMetadata( poolTimeSeriesTwoMetadata )
                                                             .build();

        PoolOfPairs<Double, Double> poolElevenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolElevenOneSeries )
                                                        .addTimeSeries( poolElevenTwoSeries )
                                                        .setMetadata( poolElevenMetadata )
                                                        .build();

        assertEquals( poolElevenExpected, poolElevenActual );
    }

    /**
     * Tests the retrieval of expected pairs for the eighteenth of eighteen pools from system test scenario505 as of
     * commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    public void testGetReturnsPoolThatContainsZeroPairs()
    {
        // Pool Eighteen actual
        // Supply all possible forecasts
        Mockito.when( this.observationRetriever.get() ).thenReturn( Stream.of( this.observations ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastOne, this.forecastTwo, this.forecastThree, this.forecastFour ) );
        Supplier<Stream<TimeSeries<Double>>> forcSupplierEighteen = CachingRetriever.of( this.forecastRetriever );

        TimeWindowOuter poolEighteenWindow = TimeWindowOuter.of( T2551_03_19T08_00_00Z, //2551-03-19T08:00:00Z
                                                                 T2551_03_19T21_00_00Z, //2551-03-19T21:00:00Z
                                                                 Duration.ofHours( 17 ),
                                                                 Duration.ofHours( 40 ) );

        SampleMetadata poolEighteenMetadata = SampleMetadata.of( this.metadata,
                                                                 poolEighteenWindow,
                                                                 this.desiredTimeScale );

        Supplier<PoolOfPairs<Double, Double>> poolEighteenSupplier =
                new PoolOfPairsSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                                .setRight( forcSupplierEighteen )
                                                                .setLeftUpscaler( this.upscaler )
                                                                .setPairer( this.pairer )
                                                                .setDesiredTimeScale( this.desiredTimeScale )
                                                                .setMetadata( poolEighteenMetadata )
                                                                .build();

        PoolOfPairs<Double, Double> poolEighteenActual = poolEighteenSupplier.get();

        // Pool Eighteen expected
        TimeSeries<Pair<Double, Double>> poolEighteenSeries =
                new TimeSeriesBuilder<Pair<Double, Double>>().setMetadata( getBoilerplateMetadata() )
                                                             .build();

        PoolOfPairs<Double, Double> poolEighteenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolEighteenSeries )
                                                        .setMetadata( poolEighteenMetadata )
                                                        .build();

        assertEquals( poolEighteenExpected, poolEighteenActual );
    }

}
