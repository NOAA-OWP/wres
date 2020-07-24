package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.MissingValues;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.sampledata.SampleMetadata.Builder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.time.TimeWindowOuter;
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

    /**
     * Streamflow for metadata.
     */

    private static final String STREAMFLOW = "Streamflow";

    /**
     * Duration for metadata.
     */

    private static final String DURATION = "DURATION";

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void testApplyOneStatisticPerInstance() throws MetricParameterException
    {
        // Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "1985-01-02T00:00:00Z" ),
                                                     Duration.ofHours( 6 ),
                                                     Duration.ofHours( 18 ) );
        TimeWindowOuter timeWindow = window;

        SampleMetadata m1 = new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                        .setIdentifier( DatasetIdentifier.of( MetricTestDataFactory.getLocation( "A" ),
                                                                                              STREAMFLOW ) )
                                                        .setTimeWindow( timeWindow )
                                                        .build();
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
                                                                                   .build();

        DurationScoreStatisticComponent component = DurationScoreStatisticComponent.newBuilder()
                                                                                   .setMetric( metricComponent )
                                                                                   .setValue( expectedSource )
                                                                                   .build();

        DurationScoreMetric metric = DurationScoreMetric.newBuilder()
                                                        .setName( MetricName.TIME_TO_PEAK_ERROR_STATISTIC )
                                                        .build();

        DurationScoreStatistic score = DurationScoreStatistic.newBuilder()
                                                             .setMetric( metric )
                                                             .addStatistics( component )
                                                             .build();

        DurationScoreStatisticOuter expected = DurationScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );

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

        // Metadata for the output
        TimeWindowOuter window = TimeWindowOuter.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                     Instant.parse( "1985-01-02T00:00:00Z" ),
                                                     Duration.ofHours( 6 ),
                                                     Duration.ofHours( 18 ) );
        TimeWindowOuter timeWindow = window;

        SampleMetadata m1 = new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                        .setIdentifier( DatasetIdentifier.of( MetricTestDataFactory.getLocation( "A" ),
                                                                                              STREAMFLOW ) )
                                                        .setTimeWindow( timeWindow )
                                                        .build();

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

        DurationScoreMetricComponent meanMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                       .setName( ComponentName.MEAN )
                                                                                       .build();

        DurationScoreMetricComponent minMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                      .setName( ComponentName.MINIMUM )
                                                                                      .build();

        DurationScoreMetricComponent maxMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                      .setName( ComponentName.MAXIMUM )
                                                                                      .build();

        DurationScoreMetricComponent meanAbsMetricComponent = DurationScoreMetricComponent.newBuilder()
                                                                                          .setName( ComponentName.MEAN_ABSOLUTE )
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
        DurationScoreStatistic score = DurationScoreStatistic.newBuilder()
                                                             .setMetric( metric )
                                                             .addStatistics( meanComponent )
                                                             .addStatistics( minComponent )
                                                             .addStatistics( maxComponent )
                                                             .addStatistics( meanAbsComponent )
                                                             .build();

        DurationScoreStatisticOuter actual = ttps.apply( peakError.apply( input ) );
        DurationScoreStatisticOuter expected = DurationScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    @Test
    public void testApplyWithNoData() throws MetricParameterException
    {
        // Generate some data
        PoolOfPairs<Double, Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        // Metadata for the output
        TimeWindowOuter window = TimeWindowOuter.of( Instant.MIN,
                                                     Instant.MAX );
        TimeWindowOuter timeWindow = window;

        SampleMetadata m1 = new SampleMetadata.Builder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                        .setIdentifier( DatasetIdentifier.of( MetricTestDataFactory.getLocation( "A" ),
                                                                                              STREAMFLOW ) )
                                                        .setTimeWindow( timeWindow )
                                                        .build();

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
        DurationScoreStatistic score = DurationScoreStatistic.newBuilder()
                                                             .setMetric( metric )
                                                             .build();

        DurationScoreStatisticOuter expected = DurationScoreStatisticOuter.of( score, m1 );

        assertEquals( expected, actual );
    }

    @Test
    public void testExceptionOnNullStatistics() throws MetricParameterException
    {
        // Missing statistic
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify a non-null container of summary statistics." );

        //Build the metric
        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC, null );
    }

    @Test
    public void testExceptionOnEmptyStatistics() throws MetricParameterException
    {
        // Empty statistic
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify one or more summary statistics." );

        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC, Collections.emptySet() );
    }

    @Test
    public void testExceptionOnNullStatistic() throws MetricParameterException
    {
        // Null statistic
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot build the metric with a null statistic." );

        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC, Collections.singleton( null ) );
    }

    @Test
    public void testExceptionOnUnrecognizedStatistic() throws MetricParameterException
    {
        // Unrecognized statistic
        exception.expect( IllegalArgumentException.class );

        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                          Collections.singleton( MetricConstants.NONE ) );
    }

    @Test
    public void testExceptionOnNullIdentifier() throws MetricParameterException
    {
        // Null identifier
        exception.expect( MetricParameterException.class );

        TimingErrorDurationStatistics.of( null,
                                          Collections.singleton( MetricConstants.MEAN ) );
    }

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        // Null input to apply
        exception.expect( SampleDataException.class );

        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                          Collections.singleton( MetricConstants.MEAN ) )
                                     .apply( null );
    }

}
