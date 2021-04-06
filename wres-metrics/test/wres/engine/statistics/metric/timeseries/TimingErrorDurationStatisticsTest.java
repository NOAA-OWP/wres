package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.PoolException;
import wres.datamodel.pools.pairs.PoolOfPairs;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent;
import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic.DurationScoreStatisticComponent;
import wres.statistics.generated.MetricName;

/**
 * Tests the {@link TimingErrorDurationStatistics}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimingErrorDurationStatisticsTest
{

    @Test
    public void testApplyOneStatisticPerInstance() throws MetricParameterException
    {
        // Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Build a metric
        TimeToPeakError peakError = TimeToPeakError.of();

        // Check the results
        DurationScoreStatisticOuter actual =
                TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                  Collections.singleton( MetricConstants.MEAN ) )
                                             .apply( peakError.apply( input ) );

        com.google.protobuf.Duration expectedSource = MessageFactory.parse( Duration.ofHours( 3 ) );

        DurationScoreMetricComponent metricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                   .setName( ComponentName.MEAN )
                                                                                   .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( Long.MIN_VALUE ) )
                                                                                   .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( Long.MAX_VALUE )
                                                                                                                            .setNanos( 999_999_999 ) )
                                                                                   .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                            .setSeconds( 0 ) )
                                                                                   .build();

        DurationScoreStatisticComponent component = DurationScoreStatisticComponent.newBuilder()
                                                                                   .setMetric( metricComponent )
                                                                                   .setValue( expectedSource )
                                                                                   .build();

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();

        DurationScoreStatistic expected = DurationScoreStatistic.newBuilder()
                                                                .setMetric( metric )
                                                                .addStatistics( component )
                                                                .build();

        assertEquals( expected, actual.getData() );

        // Check some additional statistics
        // Maximum error = 12
        DurationScoreStatisticOuter max =
                TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                  Collections.singleton( MetricConstants.MAXIMUM ) )
                                             .apply( peakError.apply( input ) );

        assertEquals( MessageFactory.parse( Duration.ofHours( 12 ) ),
                      max.getComponent( MetricConstants.MAXIMUM ).getData().getValue() );

        // Minimum error = -6
        DurationScoreStatisticOuter min =
                TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                  Collections.singleton( MetricConstants.MINIMUM ) )
                                             .apply( peakError.apply( input ) );

        assertEquals( MessageFactory.parse( Duration.ofHours( -6 ) ),
                      min.getComponent( MetricConstants.MINIMUM ).getData().getValue() );

        // Mean absolute error = 9
        DurationScoreStatisticOuter meanAbs =
                TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                  Collections.singleton( MetricConstants.MEAN_ABSOLUTE ) )
                                             .apply( peakError.apply( input ) );

        assertEquals( MessageFactory.parse( Duration.ofHours( 9 ) ),
                      meanAbs.getComponent( MetricConstants.MEAN_ABSOLUTE ).getData().getValue() );
    }

    @Test
    public void testApplyMultipleStatisticInOneInstance() throws MetricParameterException
    {
        // Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Build a metric
        TimeToPeakError peakError = TimeToPeakError.of();

        // Build the summary statistics
        TimingErrorDurationStatistics ttps =
                TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                  new HashSet<>( Arrays.asList( MetricConstants.MEAN,
                                                                                MetricConstants.MAXIMUM,
                                                                                MetricConstants.MINIMUM,
                                                                                MetricConstants.MEAN_ABSOLUTE ) ) );

        com.google.protobuf.Duration expectedMean = MessageFactory.parse( Duration.ofHours( 3 ) );
        com.google.protobuf.Duration expectedMin = MessageFactory.parse( Duration.ofHours( -6 ) );
        com.google.protobuf.Duration expectedMax = MessageFactory.parse( Duration.ofHours( 12 ) );
        com.google.protobuf.Duration expectedMeanAbs = MessageFactory.parse( Duration.ofHours( 9 ) );

        DurationScoreMetricComponent baseMetric =
                DurationScoreMetricComponent.newBuilder()
                                            .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( Long.MIN_VALUE ) )
                                            .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( Long.MAX_VALUE )
                                                                                     .setNanos( 999_999_999 ) )
                                            .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                     .setSeconds( 0 ) )
                                            .build();

        DurationScoreMetricComponent meanMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                       .setName( ComponentName.MEAN )
                                                                                       .build();

        DurationScoreMetricComponent minMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                      .setName( ComponentName.MINIMUM )
                                                                                      .build();

        DurationScoreMetricComponent maxMetricComponent = DurationScoreMetricComponent.newBuilder( baseMetric )
                                                                                      .setName( ComponentName.MAXIMUM )
                                                                                      .build();

        DurationScoreMetricComponent meanAbsMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                          .setName( ComponentName.MEAN_ABSOLUTE )
                                                                                          .setMinimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds( 0 ) )
                                                                                          .setMaximum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds( Long.MAX_VALUE )
                                                                                                                                   .setNanos( 999_999_999 ) )
                                                                                          .setOptimum( com.google.protobuf.Duration.newBuilder()
                                                                                                                                   .setSeconds( 0 ) )
                                                                                          .build();

        DurationScoreStatisticComponent meanComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                       .setMetric( meanMetricComponent )
                                                                                       .setValue( expectedMean )
                                                                                       .build();

        DurationScoreStatisticComponent minComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                      .setMetric( minMetricComponent )
                                                                                      .setValue( expectedMin )
                                                                                      .build();

        DurationScoreStatisticComponent maxComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                      .setMetric( maxMetricComponent )
                                                                                      .setValue( expectedMax )
                                                                                      .build();

        DurationScoreStatisticComponent meanAbsComponent = DurationScoreStatisticComponent.newBuilder()
                                                                                          .setMetric( meanAbsMetricComponent )
                                                                                          .setValue( expectedMeanAbs )
                                                                                          .build();

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();
        DurationScoreStatistic expected = DurationScoreStatistic.newBuilder()
                                                                .setMetric( metric )
                                                                .addStatistics( meanComponent )
                                                                .addStatistics( minComponent )
                                                                .addStatistics( maxComponent )
                                                                .addStatistics( meanAbsComponent )
                                                                .build();

        DurationScoreStatisticOuter actual = ttps.apply( peakError.apply( input ) );

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testApplyWithNoData() throws MetricParameterException
    {
        // Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        // Build a metric
        TimeToPeakError peakError = TimeToPeakError.of();

        // Build the summary statistics
        TimingErrorDurationStatistics ttps =
                TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                  new HashSet<>( Arrays.asList( MetricConstants.MEAN ) ) );

        // Check the results
        DurationScoreStatisticOuter actual = ttps.apply( peakError.apply( input ) );

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();
        DurationScoreStatistic expected = DurationScoreStatistic.newBuilder()
                                                                .setMetric( metric )
                                                                .build();

        assertEquals( expected, actual.getData() );
    }

    @Test
    public void testExceptionOnNullStatistics()
    {
        MetricParameterException actual =
                assertThrows( MetricParameterException.class,
                              () -> TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                      null ) );

        assertEquals( "Specify a non-null container of summary statistics.", actual.getMessage() );
    }

    @Test
    public void testExceptionOnEmptyStatistics()
    {
        MetricParameterException actual =
                assertThrows( MetricParameterException.class,
                              () -> TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                      Collections.emptySet() ) );

        assertEquals( "Specify one or more summary statistics.", actual.getMessage() );
    }

    @Test
    public void testExceptionOnNullStatistic()
    {
        MetricParameterException actual =
                assertThrows( MetricParameterException.class,
                              () -> TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                      Collections.singleton( null ) ) );

        assertEquals( "Cannot build the metric with a null statistic.", actual.getMessage() );
    }

    @Test
    public void testExceptionOnUnrecognizedStatistic()
    {
        assertThrows( IllegalArgumentException.class,
                      () -> TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                              Collections.singleton( MetricConstants.NONE ) ) );
    }

    @Test
    public void testExceptionOnNullIdentifier()
    {
        assertThrows( MetricParameterException.class,
                      () -> TimingErrorDurationStatistics.of( null,
                                                              Collections.singleton( MetricConstants.MEAN ) ) );
    }

    @Test
    public void testApplyThrowsExceptionOnNullInput()
    {
        assertThrows( PoolException.class,
                      () -> TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                              Collections.singleton( MetricConstants.MEAN ) )
                                                         .apply( null ) );

    }

}
