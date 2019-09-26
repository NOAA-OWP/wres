package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.TimeSeriesOfPairs;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeWindow;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

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
        TimeSeriesOfPairs<Double,Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 18 ) );
        final TimeWindow timeWindow = window;

        StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                       STREAMFLOW ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.get().size(),
                                      MeasurementUnit.of( DURATION ),
                                      MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                      MetricConstants.MEAN );
        // Build a metric
        TimeToPeakError peakError = TimeToPeakError.of();

        // Check the results
        DurationScoreStatistic actual = TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                          Collections.singleton( MetricConstants.MEAN ) )
                                                                     .apply( peakError.apply( input ) );
        Duration expectedSource = Duration.ofHours( 3 );
        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        DurationScoreStatistic expected = DurationScoreStatistic.of( expectedSource, m1 );

        assertEquals( expected, actual );

        // Check some additional statistics
        // Maximum error = 12
        DurationScoreStatistic max = TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                       Collections.singleton( MetricConstants.MAXIMUM ) )
                                                                  .apply( peakError.apply( input ) );

        assertEquals( Duration.ofHours( 12 ), max.getComponent( MetricConstants.MAXIMUM ).getData() );

        // Minimum error = -6
        DurationScoreStatistic min = TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                       Collections.singleton( MetricConstants.MINIMUM ) )
                                                                  .apply( peakError.apply( input ) );

        assertEquals( Duration.ofHours( -6 ), min.getComponent( MetricConstants.MINIMUM ).getData() );

        // Mean absolute error = 9
        DurationScoreStatistic meanAbs = TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                           Collections.singleton( MetricConstants.MEAN_ABSOLUTE ) )
                                                                      .apply( peakError.apply( input ) );

        assertEquals( Duration.ofHours( 9 ), meanAbs.getComponent( MetricConstants.MEAN_ABSOLUTE ).getData() );
    }

    @Test
    public void testApplyMultipleStatisticInOneInstance() throws MetricParameterException
    {
        // Generate some data
        TimeSeriesOfPairs<Double,Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 18 ) );
        final TimeWindow timeWindow = window;

        StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                       STREAMFLOW ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.get().size(),
                                      MeasurementUnit.of( DURATION ),
                                      MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                      null );

        // Build a metric
        TimeToPeakError peakError = TimeToPeakError.of();

        // Build the summary statistics
        TimingErrorDurationStatistics ttps =
                TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                  new HashSet<>( Arrays.asList( MetricConstants.MEAN,
                                                                                MetricConstants.MAXIMUM,
                                                                                MetricConstants.MINIMUM,
                                                                                MetricConstants.MEAN_ABSOLUTE ) ) );

        // Check the results
        DurationScoreStatistic actual = ttps.apply( peakError.apply( input ) );
        Duration expectedMean = Duration.ofHours( 3 );
        Duration expectedMin = Duration.ofHours( -6 );
        Duration expectedMax = Duration.ofHours( 12 );
        Duration expectedMeanAbs = Duration.ofHours( 9 );
        Map<MetricConstants, Duration> expectedSource = new EnumMap<>( MetricConstants.class );
        expectedSource.put( MetricConstants.MEAN, expectedMean );
        expectedSource.put( MetricConstants.MINIMUM, expectedMin );
        expectedSource.put( MetricConstants.MAXIMUM, expectedMax );
        expectedSource.put( MetricConstants.MEAN_ABSOLUTE, expectedMeanAbs );

        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        DurationScoreStatistic expected = DurationScoreStatistic.of( expectedSource, m1 );
        
        assertEquals(expected, actual );
    }

    @Test
    public void testApplyWithNoData() throws MetricParameterException
    {
        // Generate some data
        TimeSeriesOfPairs<Double,Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.MIN,
                                           Instant.MAX );
        final TimeWindow timeWindow = window;

        StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                       STREAMFLOW ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.get().size(),
                                      MeasurementUnit.of( DURATION ),
                                      MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                      MetricConstants.MEAN );

        // Build a metric
        TimeToPeakError peakError = TimeToPeakError.of();

        // Build the summary statistics
        TimingErrorDurationStatistics ttps =
                TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                  new HashSet<>( Arrays.asList( MetricConstants.MEAN ) ) );

        // Check the results
        DurationScoreStatistic actual = ttps.apply( peakError.apply( input ) );

        Map<MetricConstants, Duration> expectedSource = new EnumMap<>( MetricConstants.class );
        expectedSource.put( MetricConstants.MEAN, MetricConstants.MissingValues.MISSING_DURATION );

        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        DurationScoreStatistic expected = DurationScoreStatistic.of( expectedSource, m1 );

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
