package wres.io.retrieval.datashop;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.sampledata.pairs.PoolOfPairs.PoolOfPairsBuilder;
import wres.datamodel.scale.TimeScale;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;
import wres.datamodel.time.Event;
import wres.datamodel.time.ReferenceTimeType;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeSeries.TimeSeriesBuilder;
import wres.datamodel.time.TimeWindow;
import wres.io.retrieval.datashop.PoolSupplier.PoolSupplierBuilder;

/**
 * Tests the {@link PoolSupplier}.
 * 
 * @author james.brown@hydrosolved.com
 */

@Ignore // Until PoolSupplier is implemented and tests pass
@RunWith( MockitoJUnitRunner.class )
public class PoolSupplierTest
{

    @Mock
    private Retriever<TimeSeries<Double>> observationRetriever;

    @Mock
    private Retriever<TimeSeries<Double>> forecastRetriever;

    /**
     * Tests the retrieval of expected pairs in eighteen pools, in keeping with system test scenario505 as of
     * commit 725345a6e23df36d3ad2661a068f93563caa07a8.
     */

    @Test
    public void testGetEighteenPoolsReturnsExpectedPairsInEachPool()
    {
        // Begin by creating the time-series to retrieve from the mocked retrievers
        // This test is not concerned with retrieval, and slicing by time is part of retrieval. Thus, retrieval of the 
        // pool-shaped time-series is mocked. Here, the observations are not sliced on retrieval and are instead 
        // re-used across pools. However, if the observations were to be sliced, that would happen on retrieval too.

        // Observations: 25510317T00_FAKE2_observations.xml
        // One-hourly observations with a time-scale of PT1H and TimeScaleFunction.MEAN
        TimeSeries<Double> observations =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-17T00:00:00Z" ), 313.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T01:00:00Z" ), 317.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T02:00:00Z" ), 331.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T03:00:00Z" ), 347.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T04:00:00Z" ), 349.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T05:00:00Z" ), 353.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T06:00:00Z" ), 359.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T07:00:00Z" ), 367.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T08:00:00Z" ), 373.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T09:00:00Z" ), 379.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T10:00:00Z" ), 383.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T11:00:00Z" ), 389.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T12:00:00Z" ), 397.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T13:00:00Z" ), 401.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T14:00:00Z" ), 409.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T15:00:00Z" ), 419.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T16:00:00Z" ), 421.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T17:00:00Z" ), 431.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T18:00:00Z" ), 433.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T19:00:00Z" ), 439.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T20:00:00Z" ), 443.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T21:00:00Z" ), 449.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T22:00:00Z" ), 457.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T23:00:00Z" ), 461.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T00:00:00Z" ), 463.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T01:00:00Z" ), 467.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T02:00:00Z" ), 479.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T03:00:00Z" ), 487.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T04:00:00Z" ), 491.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T05:00:00Z" ), 499.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ), 503.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T07:00:00Z" ), 509.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T08:00:00Z" ), 521.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ), 523.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T10:00:00Z" ), 541.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T11:00:00Z" ), 547.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T12:00:00Z" ), 557.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T13:00:00Z" ), 563.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T14:00:00Z" ), 569.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T15:00:00Z" ), 571.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T16:00:00Z" ), 577.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T17:00:00Z" ), 587.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T18:00:00Z" ), 593.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T19:00:00Z" ), 599.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T20:00:00Z" ), 601.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T21:00:00Z" ), 607.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T22:00:00Z" ), 613.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T23:00:00Z" ), 617.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T00:00:00Z" ), 619.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T01:00:00Z" ), 631.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T02:00:00Z" ), 641.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T03:00:00Z" ), 643.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T04:00:00Z" ), 647.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T05:00:00Z" ), 653.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T06:00:00Z" ), 659.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T07:00:00Z" ), 661.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T08:00:00Z" ), 673.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T09:00:00Z" ), 677.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T10:00:00Z" ), 683.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T11:00:00Z" ), 691.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T12:00:00Z" ), 701.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T13:00:00Z" ), 709.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T14:00:00Z" ), 719.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T15:00:00Z" ), 727.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T16:00:00Z" ), 733.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T17:00:00Z" ), 739.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T18:00:00Z" ), 743.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T19:00:00Z" ), 751.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T20:00:00Z" ), 757.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T21:00:00Z" ), 761.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T22:00:00Z" ), 769.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T23:00:00Z" ), 773.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T00:00:00Z" ), 787.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T01:00:00Z" ), 797.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T02:00:00Z" ), 809.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T03:00:00Z" ), 811.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T04:00:00Z" ), 821.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T05:00:00Z" ), 823.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T06:00:00Z" ), 827.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T07:00:00Z" ), 829.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T08:00:00Z" ), 839.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T09:00:00Z" ), 853.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T10:00:00Z" ), 857.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T11:00:00Z" ), 859.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T12:00:00Z" ), 863.0 ) )
                                               .setTimeScale( TimeScale.of( Duration.ofHours( 1 ),
                                                                            TimeScaleFunction.MEAN ) )
                                               .build();

        // Forecast: 25510317T12_FAKE2_forecast.xml from (PT0S,PT23H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeScale existingTimeScale = TimeScale.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );
        TimeSeries<Double> forecastOnePartOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-17T15:00:00Z" ), 73.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T18:00:00Z" ), 79.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T21:00:00Z" ), 83.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T00:00:00Z" ), 89.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T03:00:00Z" ), 97.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ), 101.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ), 103.0 ) )
                                               .addReferenceTime( Instant.parse( "2551-03-17T12:00:00Z" ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510317T12_FAKE2_forecast.xml from (PT17H,PT40H]
        TimeSeries<Double> forecastOnePartTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ), 101.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ), 103.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T12:00:00Z" ), 107.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T15:00:00Z" ), 109.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T18:00:00Z" ), 113.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T21:00:00Z" ), 127.0 ) )
                                               .addReferenceTime( Instant.parse( "2551-03-17T12:00:00Z" ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510318T00_FAKE2_forecast.xml from (PT0S,PT23H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastTwoPartOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-17T03:00:00Z" ), 131.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T06:00:00Z" ), 137.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-17T09:00:00Z" ), 139.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T12:00:00Z" ), 149.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T15:00:00Z" ), 151.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T18:00:00Z" ), 157.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T21:00:00Z" ), 163.0 ) )
                                               .addReferenceTime( Instant.parse( "2551-03-18T00:00:00Z" ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510318T12_FAKE2_forecast.xml from (PT17H,PT40H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastTwoPartTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ), 223.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ), 227.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T12:00:00Z" ), 229.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T15:00:00Z" ), 233.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T18:00:00Z" ), 239.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T21:00:00Z" ), 241.0 ) )
                                               .addReferenceTime( Instant.parse( "2551-03-18T00:00:00Z" ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();


        // Forecast: 25510318T12_FAKE2_forecast.xml from (PT0S,PT23H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastThreePartOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-18T15:00:00Z" ), 191.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T18:00:00Z" ), 193.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-18T21:00:00Z" ), 197.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T00:00:00Z" ), 199.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T03:00:00Z" ), 211.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T06:00:00Z" ), 223.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T09:00:00Z" ), 227.0 ) )
                                               .addReferenceTime( Instant.parse( "2551-03-18T12:00:00Z" ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510318T12_FAKE2_forecast.xml from (PT17H,PT40H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastThreePartTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-19T06:00:00Z" ), 223.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T09:00:00Z" ), 227.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T12:00:00Z" ), 229.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T15:00:00Z" ), 233.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T18:00:00Z" ), 239.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T21:00:00Z" ), 241.0 ) )
                                               .addReferenceTime( Instant.parse( "2551-03-18T12:00:00Z" ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510319T00_FAKE2_forecast.xml from (PT0S,PT23H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastFourPartOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-19T03:00:00Z" ), 251.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T06:00:00Z" ), 257.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T09:00:00Z" ), 263.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T12:00:00Z" ), 269.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T15:00:00Z" ), 271.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T18:00:00Z" ), 277.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T21:00:00Z" ), 281.0 ) )
                                               .addReferenceTime( Instant.parse( "2551-03-19T00:00:00Z" ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510319T00_FAKE2_forecast.xml from (PT17H,PT40H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastFourPartTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( "2551-03-19T18:00:00Z" ), 277.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-19T21:00:00Z" ), 281.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T00:00:00Z" ), 283.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T03:00:00Z" ), 293.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T06:00:00Z" ), 307.0 ) )
                                               .addEvent( Event.of( Instant.parse( "2551-03-20T09:00:00Z" ), 311.0 ) )
                                               .addReferenceTime( Instant.parse( "2551-03-19T00:00:00Z" ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Re-using observations across all pools, so mock a retriever that caches the retrieval and returns
        // the same data each time
        Mockito.when( this.observationRetriever.getAll() ).thenReturn( Stream.of( observations ) );

        // Cached retriever: retrieve on first call, then supply cache
        SupplyOrRetrieve<TimeSeries<Double>> obsSupplier = SupplyOrRetrieve.of( this.observationRetriever, true );

        // Desired time scale
        TimeScale desiredTimeScale = TimeScale.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );

        // Basic metadata that applies across all pools
        SampleMetadata metadata = SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                     DatasetIdentifier.of( Location.of( "FAKE2" ), "STREAMFLOW" ) );

        // Pool one actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastOnePartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierOne = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolOneWindow = TimeWindow.of( Instant.parse( "2551-03-17T00:00:00Z" ), //2551-03-17T00:00:00Z
                                                  Instant.parse( "2551-03-17T13:00:00Z" ), //2551-03-17T13:00:00Z
                                                  Duration.ofHours( 0 ),
                                                  Duration.ofHours( 23 ) );

        SampleMetadata poolOneMetadata = SampleMetadata.of( metadata, poolOneWindow );

        PoolSupplier<Double, Double> poolOneSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierOne )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolOneMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolOneActual = poolOneSupplier.get();

        // Pool one expected
        TimeSeriesBuilder<Pair<Double, Double>> poolOneBuilder = new TimeSeriesBuilder<>();
        poolOneBuilder.addEvent( Event.of( Instant.parse( "2551-03-17T15:00:00Z" ), Pair.of( 409.67, 73.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-17T18:00:00Z" ), Pair.of( 428.33, 79.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-17T21:00:00Z" ), Pair.of( 443.67, 83.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T00:00:00Z" ), Pair.of( 460.33, 89.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T03:00:00Z" ), Pair.of( 477.67, 97.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ), Pair.of( 497.67, 101.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ), Pair.of( 517.67, 103.0 ) ) )
                      .addReferenceTime( Instant.parse( "2551-03-17T12:00:00Z" ),
                                         ReferenceTimeType.DEFAULT )
                      .setTimeScale( desiredTimeScale )
                      .build();

        PoolOfPairs<Double, Double> poolOneExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolOneBuilder.build() )
                                                        .setMetadata( poolOneMetadata )
                                                        .build();

        assertEquals( poolOneExpected, poolOneActual );

        // Pool two actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastOnePartTwo ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierTwo = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolTwoWindow = TimeWindow.of( Instant.parse( "2551-03-17T00:00:00Z" ), //2551-03-17T00:00:00Z
                                                  Instant.parse( "2551-03-17T13:00:00Z" ), //2551-03-17T13:00:00Z
                                                  Duration.ofHours( 17 ),
                                                  Duration.ofHours( 40 ) );

        SampleMetadata poolTwoMetadata = SampleMetadata.of( metadata, poolTwoWindow );

        PoolSupplier<Double, Double> poolTwoSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierTwo )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolTwoMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolTwoActual = poolTwoSupplier.get();

        // Pool two expected
        TimeSeriesBuilder<Pair<Double, Double>> poolTwoBuilder = new TimeSeriesBuilder<>();
        poolTwoBuilder.addEvent( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ), Pair.of( 497.67, 101.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ), Pair.of( 517.67, 103.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T12:00:00Z" ), Pair.of( 548.33, 107.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T15:00:00Z" ), Pair.of( 567.67, 109.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T18:00:00Z" ), Pair.of( 585.67, 113.0 ) ) )
                      .addEvent( Event.of( Instant.parse( "2551-03-18T21:00:00Z" ), Pair.of( 602.33, 127.0 ) ) )
                      .addReferenceTime( Instant.parse( "2551-03-17T12:00:00Z" ),
                                         ReferenceTimeType.DEFAULT )
                      .setTimeScale( desiredTimeScale )
                      .build();

        PoolOfPairs<Double, Double> poolTwoExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolTwoBuilder.build() )
                                                        .setMetadata( poolTwoMetadata )
                                                        .build();

        assertEquals( poolTwoExpected, poolTwoActual );

        // Pool three actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastTwoPartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierThree = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolThreeWindow = TimeWindow.of( Instant.parse( "2551-03-17T07:00:00Z" ), //2551-03-17T07:00:00Z
                                                    Instant.parse( "2551-03-17T20:00:00Z" ), //2551-03-17T20:00:00Z
                                                    Duration.ofHours( 0 ),
                                                    Duration.ofHours( 23 ) );

        SampleMetadata poolThreeMetadata = SampleMetadata.of( metadata, poolThreeWindow );

        PoolSupplier<Double, Double> poolThreeSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierThree )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolThreeMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolThreeActual = poolThreeSupplier.get();

        // Pool three expected
        TimeSeriesBuilder<Pair<Double, Double>> poolThreeBuilder = new TimeSeriesBuilder<>();
        poolThreeBuilder.addEvent( Event.of( Instant.parse( "2551-03-17T15:00:00Z" ), Pair.of( 409.67, 73.0 ) ) )
                        .addEvent( Event.of( Instant.parse( "2551-03-17T18:00:00Z" ), Pair.of( 428.33, 79.0 ) ) )
                        .addEvent( Event.of( Instant.parse( "2551-03-17T21:00:00Z" ), Pair.of( 443.67, 83.0 ) ) )
                        .addEvent( Event.of( Instant.parse( "2551-03-18T00:00:00Z" ), Pair.of( 460.33, 89.0 ) ) )
                        .addEvent( Event.of( Instant.parse( "2551-03-18T03:00:00Z" ), Pair.of( 477.67, 97.0 ) ) )
                        .addEvent( Event.of( Instant.parse( "2551-03-18T06:00:00Z" ), Pair.of( 497.67, 101.0 ) ) )
                        .addEvent( Event.of( Instant.parse( "2551-03-18T09:00:00Z" ), Pair.of( 517.67, 103.0 ) ) )
                        .addReferenceTime( Instant.parse( "2551-03-17T12:00:00Z" ),
                                           ReferenceTimeType.DEFAULT )
                        .setTimeScale( desiredTimeScale )
                        .build();

        PoolOfPairs<Double, Double> poolThreeExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolThreeBuilder.build() )
                                                        .setMetadata( poolThreeMetadata )
                                                        .build();

        assertEquals( poolThreeExpected, poolThreeActual );
        
        


    }


}
