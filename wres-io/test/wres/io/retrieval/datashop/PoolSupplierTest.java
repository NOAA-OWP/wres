package wres.io.retrieval.datashop;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.function.DoubleUnaryOperator;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import wres.datamodel.Slicer;
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
import wres.datamodel.time.TimeSeriesOfDoubleBasicUpscaler;
import wres.datamodel.time.TimeSeriesPairer;
import wres.datamodel.time.TimeSeriesPairerByExactTime;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.time.TimeSeriesUpscaler;
import wres.datamodel.time.TimeWindow;
import wres.io.retrieval.datashop.PoolSupplier.PoolSupplierBuilder;

/**
 * Tests the {@link PoolSupplier}.
 * 
 * @author james.brown@hydrosolved.com
 */

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
        // This test is not concerned with retrieval. Requests for data are ordinarily "pool-shaped" for efficiency,
        // unless those datasets are being re-used across many pools. Ultimately, the pairs are filtered against the
        // pool boundaries, but the responsibility for retrieval is with the retrievers and they can decide to retrieve
        // data in a pool shape. Here, retrieval of the forecast time-series is pool-shaped and mocked as such. 
        // However, the observations are not retrieved in a pool shape and are instead re-used across pools.

        // Observations: 25510317T00_FAKE2_observations.xml
        // One-hourly observations with a time-scale of PT1H and TimeScaleFunction.MEAN
        TimeSeries<Double> observations =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_17T00_00_00Z ), 313.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T01_00_00Z ), 317.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T02_00_00Z ), 331.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T03_00_00Z ), 347.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T04_00_00Z ), 349.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T05_00_00Z ), 353.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T06_00_00Z ), 359.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T07_00_00Z ), 367.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T08_00_00Z ), 373.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T09_00_00Z ), 379.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T10_00_00Z ), 383.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T11_00_00Z ), 389.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T12_00_00Z ), 397.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T13_00_00Z ), 401.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T14_00_00Z ), 409.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T15_00_00Z ), 419.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T16_00_00Z ), 421.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T17_00_00Z ), 431.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T18_00_00Z ), 433.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T19_00_00Z ), 439.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T20_00_00Z ), 443.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T21_00_00Z ), 449.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T22_00_00Z ), 457.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T23_00_00Z ), 461.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T00_00_00Z ), 463.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T01_00_00Z ), 467.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T02_00_00Z ), 479.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T03_00_00Z ), 487.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T04_00_00Z ), 491.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T05_00_00Z ), 499.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T06_00_00Z ), 503.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T07_00_00Z ), 509.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T08_00_00Z ), 521.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T09_00_00Z ), 523.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T10_00_00Z ), 541.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T11_00_00Z ), 547.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T12_00_00Z ), 557.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T13_00_00Z ), 563.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T14_00_00Z ), 569.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), 571.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T16_00_00Z ), 577.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T17_00_00Z ), 587.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), 593.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T19_00_00Z ), 599.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T20_00_00Z ), 601.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), 607.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T22_00_00Z ), 613.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T23_00_00Z ), 617.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T00_00_00Z ), 619.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T01_00_00Z ), 631.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T02_00_00Z ), 641.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), 643.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T04_00_00Z ), 647.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T05_00_00Z ), 653.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), 659.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T07_00_00Z ), 661.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T08_00_00Z ), 673.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), 677.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T10_00_00Z ), 683.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T11_00_00Z ), 691.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T12_00_00Z ), 701.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T13_00_00Z ), 709.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T14_00_00Z ), 719.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T15_00_00Z ), 727.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T16_00_00Z ), 733.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T17_00_00Z ), 739.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), 743.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T19_00_00Z ), 751.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T20_00_00Z ), 757.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), 761.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T22_00_00Z ), 769.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T23_00_00Z ), 773.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T00_00_00Z ), 787.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T01_00_00Z ), 797.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T02_00_00Z ), 809.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T03_00_00Z ), 811.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T04_00_00Z ), 821.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T05_00_00Z ), 823.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T06_00_00Z ), 827.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T07_00_00Z ), 829.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T08_00_00Z ), 839.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T09_00_00Z ), 853.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T10_00_00Z ), 857.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T11_00_00Z ), 859.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T12_00_00Z ), 863.0 ) )
                                               .setTimeScale( TimeScale.of( Duration.ofHours( 1 ),
                                                                            TimeScaleFunction.MEAN ) )
                                               .build();

        // Forecast: 25510317T12_FAKE2_forecast.xml from (PT0S,PT23H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeScale existingTimeScale = TimeScale.of( Duration.ofHours( 3 ), TimeScaleFunction.MEAN );
        TimeSeries<Double> forecastOnePartOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_17T15_00_00Z ), 73.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T18_00_00Z ), 79.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_17T21_00_00Z ), 83.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T00_00_00Z ), 89.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T03_00_00Z ), 97.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T06_00_00Z ), 101.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T09_00_00Z ), 103.0 ) )
                                               .addReferenceTime( Instant.parse( T2551_03_17T12_00_00Z ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510317T12_FAKE2_forecast.xml from (PT17H,PT40H]
        TimeSeries<Double> forecastOnePartTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_18T06_00_00Z ), 101.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T09_00_00Z ), 103.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T12_00_00Z ), 107.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), 109.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), 113.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), 127.0 ) )
                                               .addReferenceTime( Instant.parse( T2551_03_17T12_00_00Z ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510318T00_FAKE2_forecast.xml from (PT0S,PT23H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastTwoPartOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_18T03_00_00Z ), 131.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T06_00_00Z ), 137.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T09_00_00Z ), 139.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T12_00_00Z ), 149.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), 151.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), 157.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), 163.0 ) )
                                               .addReferenceTime( Instant.parse( T2551_03_18T00_00_00Z ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510318T12_FAKE2_forecast.xml from (PT17H,PT40H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastTwoPartTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), 157.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), 163.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T00_00_00Z ), 167.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), 173.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), 179.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), 181.0 ) )
                                               .addReferenceTime( Instant.parse( T2551_03_18T00_00_00Z ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510318T12_FAKE2_forecast.xml from (PT0S,PT23H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastThreePartOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), 191.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), 193.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), 197.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T00_00_00Z ), 199.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), 211.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), 223.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), 227.0 ) )
                                               .addReferenceTime( Instant.parse( T2551_03_18T12_00_00Z ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510318T12_FAKE2_forecast.xml from (PT17H,PT40H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastThreePartTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), 223.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), 227.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T12_00_00Z ), 229.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T15_00_00Z ), 233.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), 239.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), 241.0 ) )
                                               .addReferenceTime( Instant.parse( T2551_03_18T12_00_00Z ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510319T00_FAKE2_forecast.xml from (PT0S,PT23H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastFourPartOne =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), 251.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), 257.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), 263.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T12_00_00Z ), 269.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T15_00_00Z ), 271.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), 277.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), 281.0 ) )
                                               .addReferenceTime( Instant.parse( T2551_03_19T00_00_00Z ),
                                                                  ReferenceTimeType.DEFAULT )
                                               .setTimeScale( existingTimeScale )
                                               .build();

        // Forecast: 25510319T00_FAKE2_forecast.xml from (PT17H,PT40H]
        // Three-hourly forecasts with a time-scale of PT3H and a TimeScaleFunction.MEAN
        TimeSeries<Double> forecastFourPartTwo =
                new TimeSeriesBuilder<Double>().addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), 277.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), 281.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T00_00_00Z ), 283.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T03_00_00Z ), 293.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T06_00_00Z ), 307.0 ) )
                                               .addEvent( Event.of( Instant.parse( T2551_03_20T09_00_00Z ), 311.0 ) )
                                               .addReferenceTime( Instant.parse( T2551_03_19T00_00_00Z ),
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

        // Upscaler for the left data
        TimeSeriesUpscaler<Double> leftUpscaler = TimeSeriesOfDoubleBasicUpscaler.of();

        // Pairer for the left and right data
        TimeSeriesPairer<Double, Double> pairer = TimeSeriesPairerByExactTime.of();

        // Rounder to round the actual time-series values to 2 d.p. for assertion against expected
        DoubleUnaryOperator rounder = Slicer.rounder( 2 );
        UnaryOperator<Pair<Double, Double>> pairRounder =
                pair -> Pair.of( rounder.applyAsDouble( pair.getLeft() ), rounder.applyAsDouble( pair.getRight() ) );

        // Pool One actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastOnePartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierOne = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolOneWindow = TimeWindow.of( Instant.parse( T2551_03_17T00_00_00Z ), //2551-03-17T00:00:00Z
                                                  Instant.parse( T2551_03_17T13_00_00Z ), //2551-03-17T13:00:00Z
                                                  Duration.ofHours( 0 ),
                                                  Duration.ofHours( 23 ) );

        SampleMetadata poolOneMetadata = SampleMetadata.of( metadata, poolOneWindow );

        PoolSupplier<Double, Double> poolOneSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierOne )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolOneMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolOneActual = poolOneSupplier.get();

        // Round for comparison
        poolOneActual = TimeSeriesSlicer.transform( poolOneActual, pairRounder );

        // Pool One expected
        TimeSeriesBuilder<Pair<Double, Double>> poolOneBuilder = new TimeSeriesBuilder<>();
        poolOneBuilder.addEvent( Event.of( Instant.parse( T2551_03_17T15_00_00Z ), Pair.of( 409.67, 73.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_17T18_00_00Z ), Pair.of( 428.33, 79.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_17T21_00_00Z ), Pair.of( 443.67, 83.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T00_00_00Z ), Pair.of( 460.33, 89.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T03_00_00Z ), Pair.of( 477.67, 97.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T06_00_00Z ), Pair.of( 497.67, 101.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T09_00_00Z ), Pair.of( 517.67, 103.0 ) ) )
                      .addReferenceTime( Instant.parse( T2551_03_17T12_00_00Z ),
                                         ReferenceTimeType.DEFAULT )
                      .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolOneExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolOneBuilder.build() )
                                                        .setMetadata( poolOneMetadata )
                                                        .build();

        assertEquals( poolOneExpected, poolOneActual );

        // Pool Two actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastOnePartTwo ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierTwo = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolTwoWindow = TimeWindow.of( Instant.parse( T2551_03_17T00_00_00Z ), //2551-03-17T00:00:00Z
                                                  Instant.parse( T2551_03_17T13_00_00Z ), //2551-03-17T13:00:00Z
                                                  Duration.ofHours( 17 ),
                                                  Duration.ofHours( 40 ) );

        SampleMetadata poolTwoMetadata = SampleMetadata.of( metadata, poolTwoWindow );

        PoolSupplier<Double, Double> poolTwoSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierTwo )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolTwoMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolTwoActual = poolTwoSupplier.get();
        
        // Round for comparison
        poolTwoActual = TimeSeriesSlicer.transform( poolTwoActual, pairRounder );

        // Pool Two expected
        TimeSeriesBuilder<Pair<Double, Double>> poolTwoBuilder = new TimeSeriesBuilder<>();
        poolTwoBuilder.addEvent( Event.of( Instant.parse( T2551_03_18T06_00_00Z ), Pair.of( 497.67, 101.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T09_00_00Z ), Pair.of( 517.67, 103.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T12_00_00Z ), Pair.of( 548.33, 107.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), Pair.of( 567.67, 109.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), Pair.of( 585.67, 113.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), Pair.of( 602.33, 127.0 ) ) )
                      .addReferenceTime( Instant.parse( T2551_03_17T12_00_00Z ),
                                         ReferenceTimeType.DEFAULT )
                      .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolTwoExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolTwoBuilder.build() )
                                                        .setMetadata( poolTwoMetadata )
                                                        .build();

        assertEquals( poolTwoExpected, poolTwoActual );

        // Pool Three actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastOnePartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierThree = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolThreeWindow = TimeWindow.of( Instant.parse( T2551_03_17T07_00_00Z ), //2551-03-17T07:00:00Z
                                                    Instant.parse( T2551_03_17T20_00_00Z ), //2551-03-17T20:00:00Z
                                                    Duration.ofHours( 0 ),
                                                    Duration.ofHours( 23 ) );

        SampleMetadata poolThreeMetadata = SampleMetadata.of( metadata, poolThreeWindow );

        PoolSupplier<Double, Double> poolThreeSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierThree )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolThreeMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolThreeActual = poolThreeSupplier.get();
        
        // Round for comparison
        poolThreeActual = TimeSeriesSlicer.transform( poolThreeActual, pairRounder );

        // Pool Three expected
        TimeSeriesBuilder<Pair<Double, Double>> poolThreeBuilder = new TimeSeriesBuilder<>();
        poolThreeBuilder.addEvent( Event.of( Instant.parse( T2551_03_17T15_00_00Z ), Pair.of( 409.67, 73.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_17T18_00_00Z ), Pair.of( 428.33, 79.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_17T21_00_00Z ), Pair.of( 443.67, 83.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T00_00_00Z ), Pair.of( 460.33, 89.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T03_00_00Z ), Pair.of( 477.67, 97.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T06_00_00Z ), Pair.of( 497.67, 101.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T09_00_00Z ), Pair.of( 517.67, 103.0 ) ) )
                        .addReferenceTime( Instant.parse( T2551_03_17T12_00_00Z ),
                                           ReferenceTimeType.DEFAULT )
                        .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolThreeExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolThreeBuilder.build() )
                                                        .setMetadata( poolThreeMetadata )
                                                        .build();

        assertEquals( poolThreeExpected, poolThreeActual );

        // Pool Four actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastOnePartTwo ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierFour = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolFourWindow = TimeWindow.of( Instant.parse( T2551_03_17T07_00_00Z ), //2551-03-17T07:00:00Z
                                                   Instant.parse( T2551_03_17T20_00_00Z ), //2551-03-17T20:00:00Z
                                                   Duration.ofHours( 17 ),
                                                   Duration.ofHours( 40 ) );

        SampleMetadata poolFourMetadata = SampleMetadata.of( metadata, poolFourWindow );

        PoolSupplier<Double, Double> poolFourSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierFour )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolFourMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolFourActual = poolFourSupplier.get();
        
        // Round for comparison
        poolFourActual = TimeSeriesSlicer.transform( poolFourActual, pairRounder );

        // Pool Four expected
        TimeSeriesBuilder<Pair<Double, Double>> poolFourBuilder = new TimeSeriesBuilder<>();
        poolFourBuilder.addEvent( Event.of( Instant.parse( T2551_03_18T06_00_00Z ), Pair.of( 497.67, 101.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T09_00_00Z ), Pair.of( 517.67, 103.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T12_00_00Z ), Pair.of( 548.33, 107.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), Pair.of( 567.67, 109.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), Pair.of( 585.67, 113.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), Pair.of( 602.33, 127.0 ) ) )
                       .addReferenceTime( Instant.parse( T2551_03_17T12_00_00Z ),
                                          ReferenceTimeType.DEFAULT )
                       .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolFourExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolFourBuilder.build() )
                                                        .setMetadata( poolFourMetadata )
                                                        .build();

        assertEquals( poolFourExpected, poolFourActual );

        // Pool Five actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastTwoPartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierFive = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolFiveWindow = TimeWindow.of( Instant.parse( T2551_03_17T14_00_00Z ), //2551-03-17T14:00:00Z
                                                   Instant.parse( T2551_03_18T03_00_00Z ), //2551-03-18T03:00:00Z
                                                   Duration.ofHours( 0 ),
                                                   Duration.ofHours( 23 ) );

        SampleMetadata poolFiveMetadata = SampleMetadata.of( metadata, poolFiveWindow );

        PoolSupplier<Double, Double> poolFiveSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierFive )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolFiveMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolFiveActual = poolFiveSupplier.get();
        
        // Round for comparison
        poolFiveActual = TimeSeriesSlicer.transform( poolFiveActual, pairRounder );

        // Pool Five expected
        TimeSeriesBuilder<Pair<Double, Double>> poolFiveBuilder = new TimeSeriesBuilder<>();
        poolFiveBuilder.addEvent( Event.of( Instant.parse( T2551_03_17T03_00_00Z ), Pair.of( 477.67, 131.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_17T06_00_00Z ), Pair.of( 497.67, 137.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_17T09_00_00Z ), Pair.of( 517.67, 139.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T12_00_00Z ), Pair.of( 548.33, 149.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), Pair.of( 567.67, 151.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), Pair.of( 585.67, 157.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), Pair.of( 602.33, 163.0 ) ) )
                       .addReferenceTime( Instant.parse( T2551_03_18T00_00_00Z ),
                                          ReferenceTimeType.DEFAULT )
                       .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolFiveExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolFiveBuilder.build() )
                                                        .setMetadata( poolFiveMetadata )
                                                        .build();

        assertEquals( poolFiveExpected, poolFiveActual );

        // Pool Six actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastTwoPartTwo ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierSix = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolSixWindow = TimeWindow.of( Instant.parse( T2551_03_17T14_00_00Z ), //2551-03-17T14:00:00Z
                                                  Instant.parse( T2551_03_18T03_00_00Z ), //2551-03-18T03:00:00Z
                                                  Duration.ofHours( 17 ),
                                                  Duration.ofHours( 40 ) );

        SampleMetadata poolSixMetadata = SampleMetadata.of( metadata, poolSixWindow );

        PoolSupplier<Double, Double> poolSixSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierSix )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolSixMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolSixActual = poolSixSupplier.get();
        
        // Round for comparison
        poolSixActual = TimeSeriesSlicer.transform( poolSixActual, pairRounder );

        // Pool Six expected
        TimeSeriesBuilder<Pair<Double, Double>> poolSixBuilder = new TimeSeriesBuilder<>();
        poolSixBuilder.addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), Pair.of( 585.67, 157.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), Pair.of( 602.33, 163.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T00_00_00Z ), Pair.of( 616.33, 167.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), Pair.of( 638.33, 173.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), Pair.of( 653.0, 179.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), Pair.of( 670.33, 181.0 ) ) )
                      .addReferenceTime( Instant.parse( T2551_03_18T00_00_00Z ),
                                         ReferenceTimeType.DEFAULT )
                      .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolSixExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolSixBuilder.build() )
                                                        .setMetadata( poolSixMetadata )
                                                        .build();

        assertEquals( poolSixExpected, poolSixActual );

        // Pool Seven actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastTwoPartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierSeven = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolSevenWindow = TimeWindow.of( Instant.parse( T2551_03_17T21_00_00Z ), //2551-03-17T21:00:00Z
                                                    Instant.parse( T2551_03_18T10_00_00Z ), //2551-03-18T10:00:00Z
                                                    Duration.ofHours( 0 ),
                                                    Duration.ofHours( 23 ) );

        SampleMetadata poolSevenMetadata = SampleMetadata.of( metadata, poolSevenWindow );

        PoolSupplier<Double, Double> poolSevenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierSeven )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolSevenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolSevenActual = poolSevenSupplier.get();
        
        // Round for comparison
        poolSevenActual = TimeSeriesSlicer.transform( poolSevenActual, pairRounder );

        // Pool Seven expected
        TimeSeriesBuilder<Pair<Double, Double>> poolSevenBuilder = new TimeSeriesBuilder<>();
        poolSevenBuilder.addEvent( Event.of( Instant.parse( T2551_03_17T03_00_00Z ), Pair.of( 477.67, 131.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_17T06_00_00Z ), Pair.of( 497.67, 137.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_17T09_00_00Z ), Pair.of( 517.67, 139.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T12_00_00Z ), Pair.of( 548.33, 149.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), Pair.of( 567.67, 151.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), Pair.of( 585.67, 157.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), Pair.of( 602.33, 163.0 ) ) )
                        .addReferenceTime( Instant.parse( T2551_03_18T00_00_00Z ),
                                           ReferenceTimeType.DEFAULT )
                        .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolSevenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolSevenBuilder.build() )
                                                        .setMetadata( poolSevenMetadata )
                                                        .build();

        assertEquals( poolSevenExpected, poolSevenActual );

        // Pool Eight actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastTwoPartTwo ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierEight = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolEightWindow = TimeWindow.of( Instant.parse( T2551_03_17T21_00_00Z ), //2551-03-17T21:00:00Z
                                                    Instant.parse( T2551_03_18T10_00_00Z ), //2551-03-18T10:00:00Z
                                                    Duration.ofHours( 17 ),
                                                    Duration.ofHours( 40 ) );

        SampleMetadata poolEightMetadata = SampleMetadata.of( metadata, poolEightWindow );

        PoolSupplier<Double, Double> poolEightSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierEight )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolEightMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolEightActual = poolEightSupplier.get();
        
        // Round for comparison
        poolEightActual = TimeSeriesSlicer.transform( poolEightActual, pairRounder );

        // Pool Eight expected
        TimeSeriesBuilder<Pair<Double, Double>> poolEightBuilder = new TimeSeriesBuilder<>();
        poolEightBuilder.addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), Pair.of( 585.67, 157.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), Pair.of( 602.33, 163.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_19T00_00_00Z ), Pair.of( 616.33, 167.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), Pair.of( 638.33, 173.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), Pair.of( 653.0, 179.0 ) ) )
                        .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), Pair.of( 670.33, 181.0 ) ) )
                        .addReferenceTime( Instant.parse( T2551_03_18T00_00_00Z ),
                                           ReferenceTimeType.DEFAULT )
                        .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolEightExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolEightBuilder.build() )
                                                        .setMetadata( poolEightMetadata )
                                                        .build();

        assertEquals( poolEightExpected, poolEightActual );

        // Pool Nine actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastThreePartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierNine = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolNineWindow = TimeWindow.of( Instant.parse( T2551_03_18T04_00_00Z ), //2551-03-18T04:00:00Z
                                                   Instant.parse( T2551_03_18T17_00_00Z ), //2551-03-18T17:00:00Z
                                                   Duration.ofHours( 0 ),
                                                   Duration.ofHours( 23 ) );

        SampleMetadata poolNineMetadata = SampleMetadata.of( metadata, poolNineWindow );

        PoolSupplier<Double, Double> poolNineSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierNine )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolNineMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolNineActual = poolNineSupplier.get();
        
        // Round for comparison
        poolNineActual = TimeSeriesSlicer.transform( poolNineActual, pairRounder );

        // Pool Nine expected
        TimeSeriesBuilder<Pair<Double, Double>> poolNineBuilder = new TimeSeriesBuilder<>();
        poolNineBuilder.addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), Pair.of( 567.67, 191.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), Pair.of( 585.67, 193.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), Pair.of( 602.33, 197.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_19T00_00_00Z ), Pair.of( 616.33, 199.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), Pair.of( 638.33, 211.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), Pair.of( 653.0, 223.0 ) ) )
                       .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), Pair.of( 670.33, 227.0 ) ) )
                       .addReferenceTime( Instant.parse( T2551_03_18T12_00_00Z ),
                                          ReferenceTimeType.DEFAULT )
                       .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolNineExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolNineBuilder.build() )
                                                        .setMetadata( poolNineMetadata )
                                                        .build();

        assertEquals( poolNineExpected, poolNineActual );

        // Pool Ten actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastThreePartTwo ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierTen = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolTenWindow = TimeWindow.of( Instant.parse( T2551_03_18T04_00_00Z ), //2551-03-18T04:00:00Z
                                                  Instant.parse( T2551_03_18T17_00_00Z ), //2551-03-18T17:00:00Z
                                                  Duration.ofHours( 17 ),
                                                  Duration.ofHours( 40 ) );

        SampleMetadata poolTenMetadata = SampleMetadata.of( metadata, poolTenWindow );

        PoolSupplier<Double, Double> poolTenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierTen )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolTenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolTenActual = poolTenSupplier.get();
        
        // Round for comparison
        poolTenActual = TimeSeriesSlicer.transform( poolTenActual, pairRounder );

        // Pool Ten expected
        TimeSeriesBuilder<Pair<Double, Double>> poolTenBuilder = new TimeSeriesBuilder<>();
        poolTenBuilder.addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), Pair.of( 653.0, 223.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), Pair.of( 670.33, 227.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T12_00_00Z ), Pair.of( 691.67, 229.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T15_00_00Z ), Pair.of( 718.33, 233.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), Pair.of( 738.33, 239.0 ) ) )
                      .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), Pair.of( 756.33, 241.0 ) ) )
                      .addReferenceTime( Instant.parse( T2551_03_18T12_00_00Z ),
                                         ReferenceTimeType.DEFAULT )
                      .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolTenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolTenBuilder.build() )
                                                        .setMetadata( poolTenMetadata )
                                                        .build();

        assertEquals( poolTenExpected, poolTenActual );

        // Pool Eleven actual: NOTE two forecasts
        Mockito.when( this.forecastRetriever.getAll() )
               .thenReturn( Stream.of( forecastThreePartOne, forecastFourPartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierEleven = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolElevenWindow = TimeWindow.of( Instant.parse( T2551_03_18T11_00_00Z ), //2551-03-18T11:00:00Z
                                                     Instant.parse( T2551_03_19T00_00_00Z ), //2551-03-19T00:00:00Z
                                                     Duration.ofHours( 0 ),
                                                     Duration.ofHours( 23 ) );

        SampleMetadata poolElevenMetadata = SampleMetadata.of( metadata, poolElevenWindow );

        PoolSupplier<Double, Double> poolElevenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierEleven )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolElevenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolElevenActual = poolElevenSupplier.get();
        
        // Round for comparison
        poolElevenActual = TimeSeriesSlicer.transform( poolElevenActual, pairRounder );

        // Pool Eleven expected
        TimeSeriesBuilder<Pair<Double, Double>> poolElevenOneBuilder = new TimeSeriesBuilder<>();
        poolElevenOneBuilder.addEvent( Event.of( Instant.parse( T2551_03_18T15_00_00Z ), Pair.of( 567.67, 191.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_18T18_00_00Z ), Pair.of( 585.67, 193.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_18T21_00_00Z ), Pair.of( 602.33, 197.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T00_00_00Z ), Pair.of( 616.33, 199.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), Pair.of( 638.33, 211.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), Pair.of( 653.0, 223.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), Pair.of( 670.33, 227.0 ) ) )
                            .addReferenceTime( Instant.parse( T2551_03_18T12_00_00Z ),
                                               ReferenceTimeType.DEFAULT )
                            .setTimeScale( desiredTimeScale );

        TimeSeriesBuilder<Pair<Double, Double>> poolElevenTwoBuilder = new TimeSeriesBuilder<>();
        poolElevenTwoBuilder.addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), Pair.of( 638.33, 251.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), Pair.of( 653.0, 257.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), Pair.of( 670.33, 263.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T12_00_00Z ), Pair.of( 691.67, 269.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T15_00_00Z ), Pair.of( 718.33, 271.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), Pair.of( 738.33, 277.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), Pair.of( 756.33, 281.0 ) ) )
                            .addReferenceTime( Instant.parse( T2551_03_19T00_00_00Z ),
                                               ReferenceTimeType.DEFAULT )
                            .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolElevenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolElevenOneBuilder.build() )
                                                        .addTimeSeries( poolElevenTwoBuilder.build() )
                                                        .setMetadata( poolElevenMetadata )
                                                        .build();

        assertEquals( poolElevenExpected, poolElevenActual );

        // Pool Twelve actual: NOTE two forecasts
        Mockito.when( this.forecastRetriever.getAll() )
               .thenReturn( Stream.of( forecastThreePartTwo, forecastFourPartTwo ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierTwelve = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolTwelveWindow = TimeWindow.of( Instant.parse( T2551_03_18T11_00_00Z ), //2551-03-18T11:00:00Z
                                                     Instant.parse( T2551_03_19T00_00_00Z ), //2551-03-19T00:00:00Z
                                                     Duration.ofHours( 17 ),
                                                     Duration.ofHours( 40 ) );

        SampleMetadata poolTwelveMetadata = SampleMetadata.of( metadata, poolTwelveWindow );

        PoolSupplier<Double, Double> poolTwelveSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierTwelve )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolTwelveMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolTwelveActual = poolTwelveSupplier.get();
        
        // Round for comparison
        poolTwelveActual = TimeSeriesSlicer.transform( poolTwelveActual, pairRounder );

        // Pool Twelve expected
        TimeSeriesBuilder<Pair<Double, Double>> poolTwelveOneBuilder = new TimeSeriesBuilder<>();
        poolTwelveOneBuilder.addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), Pair.of( 653.0, 223.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), Pair.of( 670.33, 227.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T12_00_00Z ), Pair.of( 691.67, 229.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T15_00_00Z ), Pair.of( 718.33, 233.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), Pair.of( 738.33, 239.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), Pair.of( 756.33, 241.0 ) ) )
                            .addReferenceTime( Instant.parse( T2551_03_18T12_00_00Z ),
                                               ReferenceTimeType.DEFAULT )
                            .setTimeScale( desiredTimeScale );

        TimeSeriesBuilder<Pair<Double, Double>> poolTwelveTwoBuilder = new TimeSeriesBuilder<>();
        poolTwelveTwoBuilder.addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), Pair.of( 738.33, 277.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), Pair.of( 756.33, 281.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_20T00_00_00Z ), Pair.of( 776.33, 283.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_20T03_00_00Z ), Pair.of( 805.67, 293.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_20T06_00_00Z ), Pair.of( 823.67, 307.0 ) ) )
                            .addEvent( Event.of( Instant.parse( T2551_03_20T09_00_00Z ), Pair.of( 840.33, 311.0 ) ) )
                            .addReferenceTime( Instant.parse( T2551_03_19T00_00_00Z ),
                                               ReferenceTimeType.DEFAULT )
                            .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolTwelveExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolTwelveOneBuilder.build() )
                                                        .addTimeSeries( poolTwelveTwoBuilder.build() )
                                                        .setMetadata( poolTwelveMetadata )
                                                        .build();

        assertEquals( poolTwelveExpected, poolTwelveActual );

        // Pool Thirteen actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastFourPartOne ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierThirteen = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolThirteenWindow = TimeWindow.of( Instant.parse( T2551_03_18T18_00_00Z ), //2551-03-18T18:00:00Z
                                                       Instant.parse( T2551_03_19T07_00_00Z ), //2551-03-19T07:00:00Z
                                                       Duration.ofHours( 0 ),
                                                       Duration.ofHours( 23 ) );

        SampleMetadata poolThirteenMetadata = SampleMetadata.of( metadata, poolThirteenWindow );

        PoolSupplier<Double, Double> poolThirteenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierThirteen )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolThirteenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolThirteenActual = poolThirteenSupplier.get();

        // Round for comparison
        poolThirteenActual = TimeSeriesSlicer.transform( poolThirteenActual, pairRounder );
        
        // Pool Thirteen expected
        TimeSeriesBuilder<Pair<Double, Double>> poolThirteenBuilder = new TimeSeriesBuilder<>();
        poolThirteenBuilder.addEvent( Event.of( Instant.parse( T2551_03_19T03_00_00Z ), Pair.of( 638.33, 251.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_19T06_00_00Z ), Pair.of( 653.0, 257.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_19T09_00_00Z ), Pair.of( 670.33, 263.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_19T12_00_00Z ), Pair.of( 691.67, 269.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_19T15_00_00Z ), Pair.of( 718.33, 271.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), Pair.of( 738.33, 277.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), Pair.of( 756.33, 281.0 ) ) )
                           .addReferenceTime( Instant.parse( T2551_03_19T00_00_00Z ),
                                              ReferenceTimeType.DEFAULT )
                           .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolThirteenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolThirteenBuilder.build() )
                                                        .setMetadata( poolThirteenMetadata )
                                                        .build();

        assertEquals( poolThirteenExpected, poolThirteenActual );

        // Pool Fourteen actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of( forecastFourPartTwo ) );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierFourteen = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolFourteenWindow = TimeWindow.of( Instant.parse( T2551_03_18T18_00_00Z ), //2551-03-18T18:00:00Z
                                                       Instant.parse( T2551_03_19T07_00_00Z ), //2551-03-19T07:00:00Z
                                                       Duration.ofHours( 17 ),
                                                       Duration.ofHours( 40 ) );

        SampleMetadata poolFourteenMetadata = SampleMetadata.of( metadata, poolFourteenWindow );

        PoolSupplier<Double, Double> poolFourteenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierFourteen )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolFourteenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolFourteenActual = poolFourteenSupplier.get();
        
        // Round for comparison
        poolFourteenActual = TimeSeriesSlicer.transform( poolFourteenActual, pairRounder );

        // Pool Fourteen expected
        TimeSeriesBuilder<Pair<Double, Double>> poolFourteenBuilder = new TimeSeriesBuilder<>();
        poolFourteenBuilder.addEvent( Event.of( Instant.parse( T2551_03_19T18_00_00Z ), Pair.of( 738.33, 277.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_19T21_00_00Z ), Pair.of( 756.33, 281.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_20T00_00_00Z ), Pair.of( 776.33, 283.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_20T03_00_00Z ), Pair.of( 805.67, 293.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_20T06_00_00Z ), Pair.of( 823.67, 307.0 ) ) )
                           .addEvent( Event.of( Instant.parse( T2551_03_20T09_00_00Z ), Pair.of( 840.33, 311.0 ) ) )
                           .addReferenceTime( Instant.parse( T2551_03_19T00_00_00Z ),
                                              ReferenceTimeType.DEFAULT )
                           .setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolFourteenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolFourteenBuilder.build() )
                                                        .setMetadata( poolFourteenMetadata )
                                                        .build();

        assertEquals( poolFourteenExpected, poolFourteenActual );

        // Pool Fifteen actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of() );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierFifteen = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolFifteenWindow = TimeWindow.of( Instant.parse( T2551_03_19T01_00_00Z ), //2551-03-19T01:00:00Z
                                                      Instant.parse( T2551_03_19T14_00_00Z ), //2551-03-19T14:00:00Z
                                                      Duration.ofHours( 0 ),
                                                      Duration.ofHours( 23 ) );

        SampleMetadata poolFifteenMetadata = SampleMetadata.of( metadata, poolFifteenWindow );

        PoolSupplier<Double, Double> poolFifteenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierFifteen )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolFifteenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolFifteenActual = poolFifteenSupplier.get();

        // Pool Fifteen expected
        TimeSeriesBuilder<Pair<Double, Double>> poolFifteenBuilder = new TimeSeriesBuilder<>();
        poolFifteenBuilder.setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolFifteenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolFifteenBuilder.build() )
                                                        .setMetadata( poolFifteenMetadata )
                                                        .build();

        assertEquals( poolFifteenExpected, poolFifteenActual );

        // Pool Sixteen actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of() );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierSixteen = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolSixteenWindow = TimeWindow.of( Instant.parse( T2551_03_19T01_00_00Z ), //2551-03-19T01:00:00Z
                                                      Instant.parse( T2551_03_19T14_00_00Z ), //2551-03-19T14:00:00Z
                                                      Duration.ofHours( 17 ),
                                                      Duration.ofHours( 40 ) );

        SampleMetadata poolSixteenMetadata = SampleMetadata.of( metadata, poolSixteenWindow );

        PoolSupplier<Double, Double> poolSixteenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierSixteen )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolSixteenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolSixteenActual = poolSixteenSupplier.get();

        // Pool Sixteen expected
        TimeSeriesBuilder<Pair<Double, Double>> poolSixteenBuilder = new TimeSeriesBuilder<>();
        poolSixteenBuilder.setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolSixteenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolSixteenBuilder.build() )
                                                        .setMetadata( poolSixteenMetadata )
                                                        .build();

        assertEquals( poolSixteenExpected, poolSixteenActual );

        // Pool Seventeen actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of() );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierSeventeen = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolSeventeenWindow = TimeWindow.of( Instant.parse( T2551_03_19T08_00_00Z ), //2551-03-19T08:00:00Z
                                                        Instant.parse( T2551_03_19T21_00_00Z ), //2551-03-19T21:00:00Z
                                                        Duration.ofHours( 0 ),
                                                        Duration.ofHours( 23 ) );

        SampleMetadata poolSeventeenMetadata = SampleMetadata.of( metadata, poolSeventeenWindow );

        PoolSupplier<Double, Double> poolSeventeenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierSeventeen )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolSeventeenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolSeventeenActual = poolSeventeenSupplier.get();

        // Pool Seventeen expected
        TimeSeriesBuilder<Pair<Double, Double>> poolSeventeenBuilder = new TimeSeriesBuilder<>();
        poolSeventeenBuilder.setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolSeventeenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolSeventeenBuilder.build() )
                                                        .setMetadata( poolSeventeenMetadata )
                                                        .build();

        assertEquals( poolSeventeenExpected, poolSeventeenActual );

        // Pool Eighteen actual
        Mockito.when( this.forecastRetriever.getAll() ).thenReturn( Stream.of() );
        SupplyOrRetrieve<TimeSeries<Double>> forcSupplierEighteen = SupplyOrRetrieve.of( this.forecastRetriever );

        TimeWindow poolEighteenWindow = TimeWindow.of( Instant.parse( T2551_03_19T08_00_00Z ), //2551-03-19T08:00:00Z
                                                       Instant.parse( T2551_03_19T21_00_00Z ), //2551-03-19T21:00:00Z
                                                       Duration.ofHours( 17 ),
                                                       Duration.ofHours( 40 ) );

        SampleMetadata poolEighteenMetadata = SampleMetadata.of( metadata, poolEighteenWindow );

        PoolSupplier<Double, Double> poolEighteenSupplier =
                new PoolSupplierBuilder<Double, Double>().setLeft( obsSupplier )
                                                         .setRight( forcSupplierEighteen )
                                                         .setLeftUpscaler( leftUpscaler )
                                                         .setPairer( pairer )
                                                         .setDesiredTimeScale( desiredTimeScale )
                                                         .setMetadata( poolEighteenMetadata )
                                                         .build();

        PoolOfPairs<Double, Double> poolEighteenActual = poolEighteenSupplier.get();

        // Pool Eighteen expected
        TimeSeriesBuilder<Pair<Double, Double>> poolEighteenBuilder = new TimeSeriesBuilder<>();
        poolEighteenBuilder.setTimeScale( desiredTimeScale );

        PoolOfPairs<Double, Double> poolEighteenExpected =
                new PoolOfPairsBuilder<Double, Double>().addTimeSeries( poolEighteenBuilder.build() )
                                                        .setMetadata( poolEighteenMetadata )
                                                        .build();

        assertEquals( poolEighteenExpected, poolEighteenActual );

    }
    

    // Times used
    
    private static final String T2551_03_20T12_00_00Z = "2551-03-20T12:00:00Z";

    private static final String T2551_03_20T11_00_00Z = "2551-03-20T11:00:00Z";

    private static final String T2551_03_20T10_00_00Z = "2551-03-20T10:00:00Z";

    private static final String T2551_03_20T09_00_00Z = "2551-03-20T09:00:00Z";

    private static final String T2551_03_20T08_00_00Z = "2551-03-20T08:00:00Z";

    private static final String T2551_03_20T07_00_00Z = "2551-03-20T07:00:00Z";

    private static final String T2551_03_20T06_00_00Z = "2551-03-20T06:00:00Z";

    private static final String T2551_03_20T05_00_00Z = "2551-03-20T05:00:00Z";

    private static final String T2551_03_20T04_00_00Z = "2551-03-20T04:00:00Z";

    private static final String T2551_03_20T03_00_00Z = "2551-03-20T03:00:00Z";

    private static final String T2551_03_20T02_00_00Z = "2551-03-20T02:00:00Z";

    private static final String T2551_03_20T01_00_00Z = "2551-03-20T01:00:00Z";

    private static final String T2551_03_20T00_00_00Z = "2551-03-20T00:00:00Z";

    private static final String T2551_03_19T23_00_00Z = "2551-03-19T23:00:00Z";

    private static final String T2551_03_19T22_00_00Z = "2551-03-19T22:00:00Z";

    private static final String T2551_03_19T21_00_00Z = "2551-03-19T21:00:00Z";

    private static final String T2551_03_19T20_00_00Z = "2551-03-19T20:00:00Z";

    private static final String T2551_03_19T19_00_00Z = "2551-03-19T19:00:00Z";

    private static final String T2551_03_19T18_00_00Z = "2551-03-19T18:00:00Z";

    private static final String T2551_03_19T17_00_00Z = "2551-03-19T17:00:00Z";

    private static final String T2551_03_19T16_00_00Z = "2551-03-19T16:00:00Z";

    private static final String T2551_03_19T15_00_00Z = "2551-03-19T15:00:00Z";

    private static final String T2551_03_19T14_00_00Z = "2551-03-19T14:00:00Z";

    private static final String T2551_03_19T13_00_00Z = "2551-03-19T13:00:00Z";

    private static final String T2551_03_19T12_00_00Z = "2551-03-19T12:00:00Z";

    private static final String T2551_03_19T11_00_00Z = "2551-03-19T11:00:00Z";

    private static final String T2551_03_19T10_00_00Z = "2551-03-19T10:00:00Z";

    private static final String T2551_03_19T09_00_00Z = "2551-03-19T09:00:00Z";

    private static final String T2551_03_19T08_00_00Z = "2551-03-19T08:00:00Z";

    private static final String T2551_03_19T07_00_00Z = "2551-03-19T07:00:00Z";

    private static final String T2551_03_19T06_00_00Z = "2551-03-19T06:00:00Z";

    private static final String T2551_03_19T05_00_00Z = "2551-03-19T05:00:00Z";

    private static final String T2551_03_19T04_00_00Z = "2551-03-19T04:00:00Z";

    private static final String T2551_03_19T03_00_00Z = "2551-03-19T03:00:00Z";

    private static final String T2551_03_19T02_00_00Z = "2551-03-19T02:00:00Z";

    private static final String T2551_03_19T01_00_00Z = "2551-03-19T01:00:00Z";

    private static final String T2551_03_19T00_00_00Z = "2551-03-19T00:00:00Z";

    private static final String T2551_03_18T23_00_00Z = "2551-03-18T23:00:00Z";

    private static final String T2551_03_18T22_00_00Z = "2551-03-18T22:00:00Z";

    private static final String T2551_03_18T21_00_00Z = "2551-03-18T21:00:00Z";

    private static final String T2551_03_18T20_00_00Z = "2551-03-18T20:00:00Z";

    private static final String T2551_03_18T19_00_00Z = "2551-03-18T19:00:00Z";

    private static final String T2551_03_18T18_00_00Z = "2551-03-18T18:00:00Z";

    private static final String T2551_03_18T17_00_00Z = "2551-03-18T17:00:00Z";

    private static final String T2551_03_18T16_00_00Z = "2551-03-18T16:00:00Z";

    private static final String T2551_03_18T15_00_00Z = "2551-03-18T15:00:00Z";

    private static final String T2551_03_18T14_00_00Z = "2551-03-18T14:00:00Z";

    private static final String T2551_03_18T13_00_00Z = "2551-03-18T13:00:00Z";

    private static final String T2551_03_18T12_00_00Z = "2551-03-18T12:00:00Z";

    private static final String T2551_03_18T11_00_00Z = "2551-03-18T11:00:00Z";

    private static final String T2551_03_18T10_00_00Z = "2551-03-18T10:00:00Z";

    private static final String T2551_03_18T09_00_00Z = "2551-03-18T09:00:00Z";

    private static final String T2551_03_18T08_00_00Z = "2551-03-18T08:00:00Z";

    private static final String T2551_03_18T07_00_00Z = "2551-03-18T07:00:00Z";

    private static final String T2551_03_18T06_00_00Z = "2551-03-18T06:00:00Z";

    private static final String T2551_03_18T05_00_00Z = "2551-03-18T05:00:00Z";

    private static final String T2551_03_18T04_00_00Z = "2551-03-18T04:00:00Z";

    private static final String T2551_03_18T03_00_00Z = "2551-03-18T03:00:00Z";

    private static final String T2551_03_18T02_00_00Z = "2551-03-18T02:00:00Z";

    private static final String T2551_03_18T01_00_00Z = "2551-03-18T01:00:00Z";

    private static final String T2551_03_18T00_00_00Z = "2551-03-18T00:00:00Z";

    private static final String T2551_03_17T23_00_00Z = "2551-03-17T23:00:00Z";

    private static final String T2551_03_17T22_00_00Z = "2551-03-17T22:00:00Z";

    private static final String T2551_03_17T21_00_00Z = "2551-03-17T21:00:00Z";

    private static final String T2551_03_17T20_00_00Z = "2551-03-17T20:00:00Z";

    private static final String T2551_03_17T19_00_00Z = "2551-03-17T19:00:00Z";

    private static final String T2551_03_17T18_00_00Z = "2551-03-17T18:00:00Z";

    private static final String T2551_03_17T17_00_00Z = "2551-03-17T17:00:00Z";

    private static final String T2551_03_17T16_00_00Z = "2551-03-17T16:00:00Z";

    private static final String T2551_03_17T15_00_00Z = "2551-03-17T15:00:00Z";

    private static final String T2551_03_17T14_00_00Z = "2551-03-17T14:00:00Z";

    private static final String T2551_03_17T13_00_00Z = "2551-03-17T13:00:00Z";

    private static final String T2551_03_17T12_00_00Z = "2551-03-17T12:00:00Z";

    private static final String T2551_03_17T11_00_00Z = "2551-03-17T11:00:00Z";

    private static final String T2551_03_17T10_00_00Z = "2551-03-17T10:00:00Z";

    private static final String T2551_03_17T09_00_00Z = "2551-03-17T09:00:00Z";

    private static final String T2551_03_17T08_00_00Z = "2551-03-17T08:00:00Z";

    private static final String T2551_03_17T07_00_00Z = "2551-03-17T07:00:00Z";

    private static final String T2551_03_17T06_00_00Z = "2551-03-17T06:00:00Z";

    private static final String T2551_03_17T05_00_00Z = "2551-03-17T05:00:00Z";

    private static final String T2551_03_17T04_00_00Z = "2551-03-17T04:00:00Z";

    private static final String T2551_03_17T03_00_00Z = "2551-03-17T03:00:00Z";

    private static final String T2551_03_17T02_00_00Z = "2551-03-17T02:00:00Z";

    private static final String T2551_03_17T01_00_00Z = "2551-03-17T01:00:00Z";

    private static final String T2551_03_17T00_00_00Z = "2551-03-17T00:00:00Z";    

}
