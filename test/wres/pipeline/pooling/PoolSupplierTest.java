package wres.pipeline.pooling;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.CrossPairMethod;
import wres.config.yaml.components.CrossPairScope;
import wres.config.yaml.components.GeneratedBaseline;
import wres.config.yaml.components.GeneratedBaselineBuilder;
import wres.datamodel.Climatology;
import wres.datamodel.baselines.BaselineGenerator;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.scale.TimeScaleOuter;
import wres.datamodel.space.FeatureGroup;
import wres.datamodel.space.FeatureTuple;
import wres.datamodel.space.Feature;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeriesCrossPairer;
import wres.datamodel.time.TimeSeriesMetadata;
import wres.datamodel.time.TimeSeriesOfDoubleUpscaler;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.baselines.PersistenceGenerator;
import wres.io.retrieving.CachingRetriever;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.TimeWindow;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.ReferenceTime.ReferenceTimeType;

/**
 * Tests the {@link PoolSupplier}.
 *
 * @author James Brown
 */

class PoolSupplierTest
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
    private static final Geometry GEOMETRY = wres.statistics.MessageFactory.getGeometry( "DRRC2" );
    private static final Geometry ANOTHER_GEOMETRY = wres.statistics.MessageFactory.getGeometry( "DRRC3" );
    private static final Feature FEATURE = Feature.of( GEOMETRY );
    private static final Feature ANOTHER_FEATURE = Feature.of( ANOTHER_GEOMETRY );
    private static final String UNIT = "CMS";

    /** Retriever for observations.*/
    @Mock
    private Supplier<Stream<TimeSeries<Double>>> observationRetriever;

    /** Retriever for forecasts. */
    @Mock
    private Supplier<Stream<TimeSeries<Double>>> forecastRetriever;

    /** Observations. */
    private TimeSeries<Double> observations;

    /** Observations that encompass the first half of {@link #forecastOne}. */
    private TimeSeries<Double> observationsOne;

    /** Observations that encompass the second half of {@link #forecastOne}. */
    private TimeSeries<Double> observationsTwo;

    /** Observations that encompass a second feature. */
    private TimeSeries<Double> observationsThree;

    /** Forecast: 25510317T12_FAKE2_forecast.xml */
    private TimeSeries<Double> forecastOne;

    /** Forecast: 25510318T00_FAKE2_forecast.xml */
    private TimeSeries<Double> forecastTwo;

    /** Forecast: 25510318T12_FAKE2_forecast.xml */
    private TimeSeries<Double> forecastThree;

    /** Forecast: 25510319T00_FAKE2_forecast.xml */
    private TimeSeries<Double> forecastFour;

    /** Forecast that encompasses a second feature */
    private TimeSeries<Double> forecastFive;

    /** Another forecast that encompasses a second feature */
    private TimeSeries<Double> forecastSix;

    /** Desired time scale. */
    private TimeScaleOuter desiredTimeScale;

    /** Metadata for a single-feature pool. */
    private PoolMetadata metadata;

    /** Metadata for a multi-feature pool. */
    private PoolMetadata multiFeatureMetadata;

    /** Metadata for a another multi-feature pool. */
    private PoolMetadata anotherMultiFeatureMetadata;

    /** An upscaler. */
    private TimeSeriesUpscaler<Double> upscaler;

    /** A pairer. */
    private TimeSeriesPairer<Double, Double> pairer;

    @BeforeEach
    void runBeforeEachTest()
    {
        MockitoAnnotations.openMocks( this );
        TimeScaleOuter existingTimeScale = TimeScaleOuter.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );

        // Observations: 25510317T00_FAKE2_observations.xml
        TimeSeriesMetadata metadata =
                getBoilerplateMetadataWithTimeScale( FEATURE,
                                                     TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                        TimeScaleFunction.MEAN ) );
        this.observations =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
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

        this.observationsOne =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
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
                                                .build();

        this.observationsTwo =
                new TimeSeries.Builder<Double>().setMetadata( metadata )
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
                                                .build();

        TimeSeriesMetadata otherMetadata =
                getBoilerplateMetadataWithTimeScale( ANOTHER_FEATURE,
                                                     TimeScaleOuter.of( Duration.ofHours( 1 ),
                                                                        TimeScaleFunction.MEAN ) );

        this.observationsThree = TimeSeries.of( otherMetadata, this.observations.getEvents() );


        // Forecast: 25510317T12_FAKE2_forecast.xml
        TimeSeriesMetadata forecastOneMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          FEATURE,
                                                          existingTimeScale );
        this.forecastOne =
                new TimeSeries.Builder<Double>().setMetadata( forecastOneMetadata )
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
                                                          FEATURE,
                                                          existingTimeScale );
        this.forecastTwo =
                new TimeSeries.Builder<Double>().setMetadata( forecastTwoMetadata )
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
                                                          FEATURE,
                                                          existingTimeScale );
        this.forecastThree =
                new TimeSeries.Builder<Double>().setMetadata( forecastThreeMetadata )
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
                                                          FEATURE,
                                                          existingTimeScale );
        this.forecastFour =
                new TimeSeries.Builder<Double>().setMetadata( forecastFourMetadata )
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

        TimeSeriesMetadata forecastFiveMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          ANOTHER_FEATURE,
                                                          existingTimeScale );

        this.forecastFive =
                new TimeSeries.Builder<Double>().setMetadata( forecastFiveMetadata )
                                                .addEvent( Event.of( T2551_03_17T15_00_00Z, 1.0 ) )
                                                .addEvent( Event.of( T2551_03_17T18_00_00Z, 2.0 ) )
                                                .addEvent( Event.of( T2551_03_17T21_00_00Z, 3.0 ) )
                                                .addEvent( Event.of( T2551_03_18T00_00_00Z, 4.0 ) )
                                                .addEvent( Event.of( T2551_03_18T03_00_00Z, 5.0 ) )
                                                .addEvent( Event.of( T2551_03_18T06_00_00Z, 6.0 ) )
                                                .addEvent( Event.of( T2551_03_18T09_00_00Z, 7.0 ) )
                                                .addEvent( Event.of( T2551_03_18T12_00_00Z, 8.0 ) )
                                                .addEvent( Event.of( T2551_03_18T15_00_00Z, 9.0 ) )
                                                .addEvent( Event.of( T2551_03_18T18_00_00Z, 10.0 ) )
                                                .addEvent( Event.of( T2551_03_18T21_00_00Z, 11.0 ) )
                                                .build();

        this.forecastSix =
                new TimeSeries.Builder<Double>().setMetadata( forecastFiveMetadata )
                                                .addEvent( Event.of( T2551_03_17T15_00_00Z, 1.0 ) )
                                                .addEvent( Event.of( T2551_03_17T18_00_00Z, 2.0 ) )
                                                .build();

        // Desired time scale
        this.desiredTimeScale = TimeScaleOuter.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );

        // Basic metadata
        Evaluation evaluation = Evaluation.newBuilder()
                                          .setRightVariableName( "STREAMFLOW" )
                                          .setMeasurementUnit( UNIT )
                                          .build();

        GeometryTuple geoTuple = MessageFactory.getGeometryTuple( FEATURE, FEATURE, FEATURE );
        FeatureTuple featureTuple = FeatureTuple.of( geoTuple );
        GeometryGroup geoGroup =
                wres.statistics.MessageFactory.getGeometryGroup( featureTuple.toStringShort(), geoTuple );
        FeatureGroup featureGroup = FeatureGroup.of( geoGroup );

        wres.statistics.generated.Pool pool = MessageFactory.getPool( featureGroup,
                                                                      null,
                                                                      null,
                                                                      null,
                                                                      false );

        this.metadata = PoolMetadata.of( evaluation, pool );

        GeometryTuple anotherGeoTuple = MessageFactory.getGeometryTuple( ANOTHER_FEATURE, FEATURE, FEATURE );
        FeatureTuple anotherFeatureTuple = FeatureTuple.of( anotherGeoTuple );
        GeometryTuple yetAnotherGeoTuple = MessageFactory.getGeometryTuple( FEATURE, ANOTHER_FEATURE, FEATURE );
        FeatureTuple yetAnotherFeatureTuple = FeatureTuple.of( yetAnotherGeoTuple );

        GeometryGroup anotherGeoGroup = MessageFactory.getGeometryGroup( "FOO_GROUP",
                                                                         Set.of( featureTuple,
                                                                                 anotherFeatureTuple,
                                                                                 yetAnotherFeatureTuple ) );
        FeatureGroup anotherFeatureGroup = FeatureGroup.of( anotherGeoGroup );

        wres.statistics.generated.Pool anotherPool = MessageFactory.getPool( anotherFeatureGroup,
                                                                             null,
                                                                             null,
                                                                             null,
                                                                             false );

        this.multiFeatureMetadata = PoolMetadata.of( evaluation, anotherPool );

        GeometryTuple oneMoreGeoTuple = MessageFactory.getGeometryTuple( ANOTHER_FEATURE,
                                                                         ANOTHER_FEATURE,
                                                                         ANOTHER_FEATURE );
        FeatureTuple oneMoreFeatureTuple = FeatureTuple.of( oneMoreGeoTuple );

        GeometryGroup oneMoreGeoGroup = MessageFactory.getGeometryGroup( "BAR_GROUP",
                                                                         Set.of( featureTuple,
                                                                                 oneMoreFeatureTuple ) );
        FeatureGroup oneMoreFeatureGroup = FeatureGroup.of( oneMoreGeoGroup );

        wres.statistics.generated.Pool oneMorePool = MessageFactory.getPool( oneMoreFeatureGroup,
                                                                             null,
                                                                             null,
                                                                             null,
                                                                             false );

        this.anotherMultiFeatureMetadata = PoolMetadata.of( evaluation, oneMorePool );

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
    void testGetReturnsPoolThatContainsSevenPairsInOneSeries()
    {
        // Pool One actual        
        Mockito.when( this.observationRetriever.get() ).thenReturn( Stream.of( this.observations ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() ).thenReturn( Stream.of( this.forecastOne ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierOne = this.forecastRetriever;

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_17T00_00_00Z, //2551-03-17T00:00:00Z
                                                                         T2551_03_17T13_00_00Z, //2551-03-17T13:00:00Z
                                                                         Duration.ofHours( 0 ),
                                                                         Duration.ofHours( 23 ) );
        TimeWindowOuter poolOneWindow = TimeWindowOuter.of( inner );

        PoolMetadata poolOneMetadata = PoolMetadata.of( this.metadata,
                                                        poolOneWindow,
                                                        this.desiredTimeScale );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolOneSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplierOne )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setMetadata( poolOneMetadata )
                                                                  .setBaselineShim( Function.identity() )
                                                                  .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolOneActual = poolOneSupplier.get();

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolOneSeries =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
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

        Pool<TimeSeries<Pair<Double, Double>>> poolOneExpected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolOneSeries )
                                                                    .setMetadata( poolOneMetadata )
                                                                    .build();

        assertEquals( poolOneExpected, poolOneActual );
    }

    @Test
    void testGetReturnsPoolThatContainsSevenPairsInTwoSeries()
    {
        // Pool One actual        
        Mockito.when( this.observationRetriever.get() )
               .thenReturn( Stream.of( this.observationsOne,
                                       this.observationsTwo ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() ).thenReturn( Stream.of( this.forecastOne ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierOne = this.forecastRetriever;

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_17T00_00_00Z, //2551-03-17T00:00:00Z
                                                                         T2551_03_17T13_00_00Z, //2551-03-17T13:00:00Z
                                                                         Duration.ofHours( 0 ),
                                                                         Duration.ofHours( 23 ) );
        TimeWindowOuter poolOneWindow = TimeWindowOuter.of( inner );

        PoolMetadata poolOneMetadata = PoolMetadata.of( this.metadata,
                                                        poolOneWindow,
                                                        this.desiredTimeScale );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolOneSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplierOne )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setMetadata( poolOneMetadata )
                                                                  .setBaselineShim( Function.identity() )
                                                                  .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolOneActual = poolOneSupplier.get();

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolOneSeriesOne =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
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
                                                              .build();

        TimeSeries<Pair<Double, Double>> poolOneSeriesTwo =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                              .addEvent( Event.of( T2551_03_18T06_00_00Z,
                                                                                   Pair.of( 497.6666666666667,
                                                                                            101.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T09_00_00Z,
                                                                                   Pair.of( 517.6666666666666,
                                                                                            103.0 ) ) )
                                                              .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolOneExpected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolOneSeriesOne )
                                                                    .addData( poolOneSeriesTwo )
                                                                    .setMetadata( poolOneMetadata )
                                                                    .build();

        assertEquals( poolOneExpected, poolOneActual );
    }

    /**
     * Tests the retrieval of expected pairs for the first of eighteen pools from system test scenario505 as of
     * commit 725345a6e23df36d3ad2661a068f93563caa07a8 where the pairs are baseline pairs.
     */

    @Test
    void testGetReturnsPoolThatContainsSevenPairsInOneSeriesForBaselineAndIncludesClimatology()
    {
        // Mock one return of the observed stream, even though it is used for climatology,
        // because we are adding to a CachingRetriever
        Mockito.when( this.observationRetriever.get() )
               .thenReturn( Stream.of( this.observations ) );

        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        // Using the same source for right and baseline, so allow the stream to be 
        // operated on for each of right and baseline
        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastOne ) )
               .thenReturn( Stream.of( this.forecastOne ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierOne = this.forecastRetriever;

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_17T00_00_00Z, //2551-03-17T00:00:00Z
                                                                         T2551_03_17T13_00_00Z, //2551-03-17T13:00:00Z
                                                                         Duration.ofHours( 0 ),
                                                                         Duration.ofHours( 23 ) );
        TimeWindowOuter poolOneWindow = TimeWindowOuter.of( inner );

        PoolMetadata poolOneMetadata = PoolMetadata.of( this.metadata,
                                                        poolOneWindow,
                                                        this.desiredTimeScale );

        wres.statistics.generated.Pool baselinePool = poolOneMetadata.getPool()
                                                                     .toBuilder()
                                                                     .setIsBaselinePool( true )
                                                                     .build();

        PoolMetadata poolOneMetadataBaseline = PoolMetadata.of( poolOneMetadata.getEvaluation(), baselinePool );

        List<TimeSeries<Double>> climData = obsSupplier.get()
                                                       .collect( Collectors.toList() );

        Supplier<Climatology> climatology = () -> Climatology.of( climData );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolOneSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplierOne )
                                                                  .setBaseline( forcSupplierOne )
                                                                  .setClimatology( climatology )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setMetadata( poolOneMetadata )
                                                                  .setBaselineMetadata( poolOneMetadataBaseline )
                                                                  .setBaselineShim( Function.identity() )
                                                                  .build();

        // Acquire the pools for the baseline
        Pool<TimeSeries<Pair<Double, Double>>> poolOneActual = poolOneSupplier.get()
                                                                              .getBaselineData();

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolOneSeries =
                new TimeSeries.Builder<Pair<Double, Double>>().addEvent( Event.of( T2551_03_17T15_00_00Z,
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

        Climatology expectedClimatology = new Climatology.Builder().addClimatology( FEATURE, climatologyArray, UNIT )
                                                                   .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolOneExpected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolOneSeries )
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
    void testGetReturnsPoolThatContainsFourteenPairsInTwoSeries()
    {
        // Pool Eleven actual: NOTE two forecasts
        Mockito.when( this.observationRetriever.get() ).thenReturn( Stream.of( this.observations ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastThree, this.forecastFour ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierEleven = CachingRetriever.of( this.forecastRetriever );

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_18T11_00_00Z, //2551-03-18T11:00:00Z
                                                                         T2551_03_19T00_00_00Z, //2551-03-19T00:00:00Z
                                                                         Duration.ofHours( 0 ),
                                                                         Duration.ofHours( 23 ) );
        TimeWindowOuter poolElevenWindow = TimeWindowOuter.of( inner );

        PoolMetadata poolElevenMetadata = PoolMetadata.of( this.metadata,
                                                           poolElevenWindow,
                                                           this.desiredTimeScale );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolElevenSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplierEleven )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setMetadata( poolElevenMetadata )
                                                                  .setBaselineShim( Function.identity() )
                                                                  .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolElevenActual = poolElevenSupplier.get();

        // Pool Eleven expected
        TimeSeriesMetadata poolTimeSeriesMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_18T12_00_00Z,
                                                          FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolElevenOneSeries =
                new TimeSeries.Builder<Pair<Double, Double>>().addEvent( Event.of( T2551_03_18T15_00_00Z,
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
                                                          FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolElevenTwoSeries =
                new TimeSeries.Builder<Pair<Double, Double>>().addEvent( Event.of( T2551_03_19T03_00_00Z,
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

        Pool<TimeSeries<Pair<Double, Double>>> poolElevenExpected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolElevenOneSeries )
                                                                    .addData( poolElevenTwoSeries )
                                                                    .setMetadata( poolElevenMetadata )
                                                                    .build();

        assertEquals( poolElevenExpected, poolElevenActual );
    }

    /**
     * Tests the retrieval of expected pairs for the eighteenth of eighteen pools from system test scenario505 as of
     * commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    void testGetReturnsPoolThatContainsZeroPairs()
    {
        // Pool Eighteen actual
        // Supply all possible forecasts
        Mockito.when( this.observationRetriever.get() ).thenReturn( Stream.of( this.observations ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastOne, this.forecastTwo, this.forecastThree, this.forecastFour ) );
        Supplier<Stream<TimeSeries<Double>>> forcSupplierEighteen = CachingRetriever.of( this.forecastRetriever );

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_19T08_00_00Z, //2551-03-19T08:00:00Z
                                                                         T2551_03_19T21_00_00Z, //2551-03-19T21:00:00Z
                                                                         Duration.ofHours( 17 ),
                                                                         Duration.ofHours( 40 ) );
        TimeWindowOuter poolEighteenWindow = TimeWindowOuter.of( inner );

        PoolMetadata poolEighteenMetadata = PoolMetadata.of( this.metadata,
                                                             poolEighteenWindow,
                                                             this.desiredTimeScale );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolEighteenSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplierEighteen )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setMetadata( poolEighteenMetadata )
                                                                  .setBaselineShim( Function.identity() )
                                                                  .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolEighteenActual = poolEighteenSupplier.get();

        Pool<TimeSeries<Pair<Double, Double>>> poolEighteenExpected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().setMetadata( poolEighteenMetadata )
                                                                    .build();

        assertEquals( poolEighteenExpected, poolEighteenActual );
    }

    @Test
    void testGetReturnsPoolThatContainsTwentyEightPairsInFourSeriesForTwoFeatures()
    {
        // Create the duplicate observed series for a different feature
        String featureName = "DOSC1";
        Feature feature = Feature.of(
                wres.statistics.MessageFactory.getGeometry( featureName ) );

        TimeSeriesMetadata obsMeta = this.observations.getMetadata();
        TimeSeries<Double> observationsTwo = new TimeSeries.Builder<Double>().addEvents( this.observations.getEvents() )
                                                                             .setMetadata( new TimeSeriesMetadata.Builder().setReferenceTimes(
                                                                                                                                   obsMeta.getReferenceTimes() )
                                                                                                                           .setTimeScale(
                                                                                                                                   obsMeta.getTimeScale() )
                                                                                                                           .setUnit(
                                                                                                                                   obsMeta.getUnit() )
                                                                                                                           .setVariableName(
                                                                                                                                   obsMeta.getVariableName() )
                                                                                                                           .setFeature(
                                                                                                                                   feature )
                                                                                                                           .build() )
                                                                             .build();

        Mockito.when( this.observationRetriever.get() ).thenReturn( Stream.of( this.observations, observationsTwo ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        // Create the duplicate forecast series for a different feature
        TimeSeriesMetadata forcThreeMeta = this.forecastThree.getMetadata();
        TimeSeries<Double> forecastFive = new TimeSeries.Builder<Double>().addEvents( this.forecastThree.getEvents() )
                                                                          .setMetadata( new TimeSeriesMetadata.Builder().setReferenceTimes(
                                                                                                                                forcThreeMeta.getReferenceTimes() )
                                                                                                                        .setTimeScale(
                                                                                                                                forcThreeMeta.getTimeScale() )
                                                                                                                        .setUnit(
                                                                                                                                forcThreeMeta.getUnit() )
                                                                                                                        .setVariableName(
                                                                                                                                forcThreeMeta.getVariableName() )
                                                                                                                        .setFeature(
                                                                                                                                feature )
                                                                                                                        .build() )
                                                                          .build();

        TimeSeriesMetadata forcFourMeta = this.forecastFour.getMetadata();
        TimeSeries<Double> forecastSix = new TimeSeries.Builder<Double>().addEvents( this.forecastFour.getEvents() )
                                                                         .setMetadata( new TimeSeriesMetadata.Builder().setReferenceTimes(
                                                                                                                               forcFourMeta.getReferenceTimes() )
                                                                                                                       .setTimeScale(
                                                                                                                               forcFourMeta.getTimeScale() )
                                                                                                                       .setUnit(
                                                                                                                               forcFourMeta.getUnit() )
                                                                                                                       .setVariableName(
                                                                                                                               forcFourMeta.getVariableName() )
                                                                                                                       .setFeature(
                                                                                                                               feature )
                                                                                                                       .build() )
                                                                         .build();

        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastThree, this.forecastFour, forecastFive, forecastSix ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplier = CachingRetriever.of( this.forecastRetriever );

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_18T11_00_00Z, //2551-03-18T11:00:00Z
                                                                         T2551_03_19T00_00_00Z, //2551-03-19T00:00:00Z
                                                                         Duration.ofHours( 0 ),
                                                                         Duration.ofHours( 23 ) );
        TimeWindowOuter poolWindow = TimeWindowOuter.of( inner );

        Geometry geometry = Geometry.newBuilder()
                                    .setName( featureName )
                                    .build();
        PoolMetadata start = PoolMetadata.of( this.metadata,
                                              poolWindow,
                                              this.desiredTimeScale );

        GeometryGroup geometryGroup = GeometryGroup.newBuilder()
                                                   .addGeometryTuples( GeometryTuple.newBuilder()
                                                                                    .setLeft( geometry )
                                                                                    .setRight( geometry ) )
                                                   .addGeometryTuples( GeometryTuple.newBuilder()
                                                                                    .setLeft( FEATURE.getGeometry() )
                                                                                    .setRight( FEATURE.getGeometry() ) )
                                                   .build();

        PoolMetadata poolMetadata =
                PoolMetadata.of( start.getEvaluation(),
                                 start.getPool()
                                      .toBuilder()
                                      .setGeometryGroup( geometryGroup )
                                      .build() );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplier )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setMetadata( poolMetadata )
                                                                  .setBaselineShim( Function.identity() )
                                                                  .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolActual = poolSupplier.get();

        assertEquals( 28, PoolSlicer.getEventCount( poolActual ) );
        assertEquals( 4, poolActual.get().size() );
    }

    @Test
    void testGetReturnsPoolThatContainsSevenPairsInThreeSeriesAcrossThreeFeatures()
    {
        Mockito.when( this.observationRetriever.get() )
               .thenReturn( Stream.of( this.observations,
                                       this.observationsThree ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastOne, this.forecastFive ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierOne = this.forecastRetriever;

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_17T00_00_00Z, //2551-03-17T00:00:00Z
                                                                         T2551_03_17T13_00_00Z, //2551-03-17T13:00:00Z
                                                                         Duration.ofHours( 0 ),
                                                                         Duration.ofHours( 23 ) );
        TimeWindowOuter poolOneWindow = TimeWindowOuter.of( inner );

        PoolMetadata poolOneMetadata = PoolMetadata.of( this.multiFeatureMetadata,
                                                        poolOneWindow,
                                                        this.desiredTimeScale );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolOneSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplierOne )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setMetadata( poolOneMetadata )
                                                                  .setBaselineShim( Function.identity() )
                                                                  .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolOneActual = poolOneSupplier.get();

        // Three time-series expected with L/R feature correlations of DRRC2-DRRC2, DRRC3-DRRC2 and DRRC2-DRRC3
        TimeSeriesMetadata timeSeriesMetadataOne =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> timeSeriesOne =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( timeSeriesMetadataOne )
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

        TimeSeriesMetadata timeSeriesMetadataTwo =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          ANOTHER_FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> timeSeriesTwo =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( timeSeriesMetadataTwo )
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

        TimeSeries<Pair<Double, Double>> timeSeriesThree =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( timeSeriesMetadataOne )
                                                              .addEvent( Event.of( T2551_03_17T15_00_00Z,
                                                                                   Pair.of( 409.6666666666667,
                                                                                            1.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_17T18_00_00Z,
                                                                                   Pair.of( 428.3333333333333,
                                                                                            2.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_17T21_00_00Z,
                                                                                   Pair.of( 443.6666666666667,
                                                                                            3.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T00_00_00Z,
                                                                                   Pair.of( 460.3333333333333,
                                                                                            4.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T03_00_00Z,
                                                                                   Pair.of( 477.6666666666667,
                                                                                            5.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T06_00_00Z,
                                                                                   Pair.of( 497.6666666666667,
                                                                                            6.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T09_00_00Z,
                                                                                   Pair.of( 517.6666666666666,
                                                                                            7.0 ) ) )
                                                              .build();

        List<TimeSeries<Pair<Double, Double>>> series = poolOneActual.get();

        assertAll( () -> assertEquals( 3, poolOneActual.get()
                                                       .size() ),
                   () -> assertTrue( series.contains( timeSeriesOne ) ),
                   () -> assertTrue( series.contains( timeSeriesTwo ) ),
                   () -> assertTrue( series.contains( timeSeriesThree ) ) );
    }

    @Test
    void testGetReturnsPoolThatContainsTwoPairsInTwoSeriesAcrossTwoFeaturesWithCrossPairing()
    {
        Mockito.when( this.observationRetriever.get() )
               .thenReturn( Stream.of( this.observations,
                                       this.observationsThree ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastOne, this.forecastSix ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierOne = this.forecastRetriever;

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_17T00_00_00Z, //2551-03-17T00:00:00Z
                                                                         T2551_03_17T13_00_00Z, //2551-03-17T13:00:00Z
                                                                         Duration.ofHours( 0 ),
                                                                         Duration.ofHours( 23 ) );
        TimeWindowOuter poolOneWindow = TimeWindowOuter.of( inner );

        PoolMetadata poolOneMetadata = PoolMetadata.of( this.anotherMultiFeatureMetadata,
                                                        poolOneWindow,
                                                        this.desiredTimeScale );

        TimeSeriesCrossPairer<Pair<Double, Double>> crossPairer = TimeSeriesCrossPairer.of();
        CrossPair crossPair = new CrossPair( CrossPairMethod.EXACT, CrossPairScope.ACROSS_FEATURES );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolOneSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplierOne )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setCrossPairer( crossPairer, crossPair )
                                                                  .setMetadata( poolOneMetadata )
                                                                  .setBaselineShim( Function.identity() )
                                                                  .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolOneActual = poolOneSupplier.get();

        // Two time-series expected with L/R feature correlations of DRRC2-DRRC2 and DRRC3-DRRC3
        TimeSeriesMetadata timeSeriesMetadataOne =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> timeSeriesOne =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( timeSeriesMetadataOne )
                                                              .addEvent( Event.of( T2551_03_17T15_00_00Z,
                                                                                   Pair.of( 409.6666666666667,
                                                                                            73.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_17T18_00_00Z,
                                                                                   Pair.of( 428.3333333333333,
                                                                                            79.0 ) ) )
                                                              .build();

        TimeSeriesMetadata timeSeriesMetadataTwo =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          ANOTHER_FEATURE,
                                                          this.desiredTimeScale );

        TimeSeries<Pair<Double, Double>> timeSeriesTwo =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( timeSeriesMetadataTwo )
                                                              .addEvent( Event.of( T2551_03_17T15_00_00Z,
                                                                                   Pair.of( 409.6666666666667,
                                                                                            1.0 ) ) )
                                                              .addEvent( Event.of( T2551_03_17T18_00_00Z,
                                                                                   Pair.of( 428.3333333333333,
                                                                                            2.0 ) ) )
                                                              .build();

        List<TimeSeries<Pair<Double, Double>>> series = poolOneActual.get();


        assertAll( () -> assertEquals( 2, poolOneActual.get()
                                                       .size() ),
                   () -> assertTrue( series.contains( timeSeriesOne ) ),
                   () -> assertTrue( series.contains( timeSeriesTwo ) ) );
    }

    @Test
    void testGetReturnsPoolThatContainsSevenPairsInOneSeriesWithPersistenceBaselineAndCrossPairing()
    {
        // Pool One actual        
        Mockito.when( this.observationRetriever.get() )
               .thenReturn( Stream.of( this.observations ) );
        Supplier<Stream<TimeSeries<Double>>> obsSupplier = CachingRetriever.of( this.observationRetriever );

        Mockito.when( this.forecastRetriever.get() )
               .thenReturn( Stream.of( this.forecastOne ) );

        Supplier<Stream<TimeSeries<Double>>> forcSupplierOne = this.forecastRetriever;

        TimeWindow inner = wres.statistics.MessageFactory.getTimeWindow( T2551_03_17T00_00_00Z, //2551-03-17T00:00:00Z
                                                                         T2551_03_17T13_00_00Z, //2551-03-17T13:00:00Z
                                                                         Duration.ofHours( 0 ),
                                                                         Duration.ofHours( 23 ) );
        TimeWindowOuter poolOneWindow = TimeWindowOuter.of( inner );

        PoolMetadata poolOneMetadata = PoolMetadata.of( this.metadata,
                                                        poolOneWindow,
                                                        this.desiredTimeScale );


        wres.statistics.generated.Pool baselinePool = poolOneMetadata.getPool().toBuilder()
                                                                     .setIsBaselinePool( true )
                                                                     .build();

        PoolMetadata poolOneBaselineMetadata = PoolMetadata.of( this.metadata.getEvaluation(), baselinePool );
        GeneratedBaseline persistence = GeneratedBaselineBuilder.builder()
                                                                .order( 1 )
                                                                .build();
        BaselineGenerator<Double> baselineGenerator = PersistenceGenerator.of( obsSupplier,
                                                                               this.upscaler,
                                                                               Double::isFinite,
                                                                               persistence,
                                                                               UNIT );

        Function<Set<Feature>, BaselineGenerator<Double>> featuredBaselineGenerator = in -> baselineGenerator;

        TimeSeriesCrossPairer<Pair<Double, Double>> crossPairer = TimeSeriesCrossPairer.of();
        CrossPair crossPair = new CrossPair( CrossPairMethod.EXACT, null );

        Supplier<Pool<TimeSeries<Pair<Double, Double>>>> poolOneSupplier =
                new PoolSupplier.Builder<Double, Double, Double>().setLeft( obsSupplier )
                                                                  .setRight( forcSupplierOne )
                                                                  .setLeftUpscaler( this.upscaler )
                                                                  .setPairer( this.pairer )
                                                                  .setCrossPairer( crossPairer, crossPair )
                                                                  .setBaselineGenerator( featuredBaselineGenerator )
                                                                  .setDesiredTimeScale( this.desiredTimeScale )
                                                                  .setMetadata( poolOneMetadata )
                                                                  .setBaselineMetadata( poolOneBaselineMetadata )
                                                                  .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolOneActual = poolOneSupplier.get();

        // Pool One expected
        TimeSeriesMetadata poolOneTimeSeriesMetadata =
                getBoilerplateMetadataWithT0AndTimeScale( T2551_03_17T12_00_00Z,
                                                          FEATURE,
                                                          this.desiredTimeScale );
        TimeSeries<Pair<Double, Double>> poolOneSeries =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
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

        TimeSeries<Pair<Double, Double>> poolOneBaselineSeries =
                new TimeSeries.Builder<Pair<Double, Double>>().setMetadata( poolOneTimeSeriesMetadata )
                                                              .addEvent( Event.of( T2551_03_17T15_00_00Z,
                                                                                   Pair.of( 409.6666666666667,
                                                                                            383.6666666666667 ) ) )
                                                              .addEvent( Event.of( T2551_03_17T18_00_00Z,
                                                                                   Pair.of( 428.3333333333333,
                                                                                            383.6666666666667 ) ) )
                                                              .addEvent( Event.of( T2551_03_17T21_00_00Z,
                                                                                   Pair.of( 443.6666666666667,
                                                                                            383.6666666666667 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T00_00_00Z,
                                                                                   Pair.of( 460.3333333333333,
                                                                                            383.6666666666667 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T03_00_00Z,
                                                                                   Pair.of( 477.6666666666667,
                                                                                            383.6666666666667 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T06_00_00Z,
                                                                                   Pair.of( 497.6666666666667,
                                                                                            383.6666666666667 ) ) )
                                                              .addEvent( Event.of( T2551_03_18T09_00_00Z,
                                                                                   Pair.of( 517.6666666666666,
                                                                                            383.6666666666667 ) ) )
                                                              .build();

        Pool<TimeSeries<Pair<Double, Double>>> poolOneExpected =
                new Pool.Builder<TimeSeries<Pair<Double, Double>>>().addData( poolOneSeries )
                                                                    .addDataForBaseline( poolOneBaselineSeries )
                                                                    .setMetadata( poolOneMetadata )
                                                                    .setMetadataForBaseline( poolOneBaselineMetadata )
                                                                    .build();

        assertEquals( poolOneExpected, poolOneActual );
    }

    /**
     * Produces time-series metadata from the inputs.
     * @param featureKey the feature
     * @param timeScale the timescale
     * @return the metadata
     */

    private static TimeSeriesMetadata getBoilerplateMetadataWithTimeScale( Feature featureKey,
                                                                           TimeScaleOuter timeScale )
    {
        return TimeSeriesMetadata.of( Collections.emptyMap(),
                                      timeScale,
                                      VARIABLE_NAME,
                                      featureKey,
                                      UNIT );
    }

    /**
     * Produces time-series metadata from the inputs.
     * @param t0 the forecast initialization time
     * @param featureKey the feature
     * @param timeScale the time scale
     * @return the metadata
     */

    private static TimeSeriesMetadata getBoilerplateMetadataWithT0AndTimeScale( Instant t0,
                                                                                Feature featureKey,
                                                                                TimeScaleOuter timeScale )
    {
        return TimeSeriesMetadata.of( Map.of( ReferenceTimeType.T0, t0 ),
                                      timeScale,
                                      VARIABLE_NAME,
                                      featureKey,
                                      UNIT );
    }
}
