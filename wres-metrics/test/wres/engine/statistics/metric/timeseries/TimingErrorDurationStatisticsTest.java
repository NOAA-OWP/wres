package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link TimingErrorDurationStatistics}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimingErrorDurationStatisticsTest
{

    @Rule
    public ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link TimingErrorDurationStatistics#apply(TimeSeriesOfSingleValuedPairs)} and compares the actual result 
     * to the expected result when adding one summary statistic to each instance. 
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testApplyOneStatisticPerInstance() throws MetricParameterException
    {
        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           ReferenceTime.ISSUE_TIME,
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 18 ) );

        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( Location.of( "A" ),
                                                                                              "Streamflow" ),
                                                                        window ),
                                                     input.getBasisTimes().size(),
                                                     MeasurementUnit.of( "DURATION" ),
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
        
        assertTrue( "Actual: " + actual.getComponent( MetricConstants.MEAN )
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );

        // Check some additional statistics
        // Maximum error = 12
        DurationScoreStatistic max = TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                       Collections.singleton( MetricConstants.MAXIMUM ) )
                                                                  .apply( peakError.apply( input ) );
        assertTrue( "Actual: " + max.getComponent( MetricConstants.MAXIMUM ).getData()
                    + ". Expected: "
                    + Duration.ofHours( 12 )
                    + ".",
                    max.getComponent( MetricConstants.MAXIMUM ).getData().equals( Duration.ofHours( 12 ) ) );
        // Minimum error = -6
        DurationScoreStatistic min = TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                       Collections.singleton( MetricConstants.MINIMUM ) )
                                                                  .apply( peakError.apply( input ) );
        assertTrue( "Actual: " + min.getComponent( MetricConstants.MINIMUM ).getData()
                    + ". Expected: "
                    + Duration.ofHours( -6 )
                    + ".",
                    min.getComponent( MetricConstants.MINIMUM ).getData().equals( Duration.ofHours( -6 ) ) );
        // Mean absolute error = 9
        DurationScoreStatistic meanAbs = TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                           Collections.singleton( MetricConstants.MEAN_ABSOLUTE ) )
                                                                      .apply( peakError.apply( input ) );
        assertTrue( "Actual: " + meanAbs.getComponent( MetricConstants.MEAN_ABSOLUTE ).getData()
                    + ". Expected: "
                    + Duration.ofHours( 9 )
                    + ".",
                    meanAbs.getComponent( MetricConstants.MEAN_ABSOLUTE ).getData().equals( Duration.ofHours( 9 ) ) );
    }

    /**
     * Tests the {@link TimingErrorDurationStatistics#apply(TimeSeriesOfSingleValuedPairs)} and compares the actual result 
     * to the expected result when adding multiple summary statistics to one instance. 
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testApplyMultipleStatisticInOneInstance() throws MetricParameterException
    {
        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           ReferenceTime.ISSUE_TIME,
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 18 ) );

        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( Location.of( "A" ),
                                                                                              "Streamflow" ),
                                                                        window ),
                                                     input.getBasisTimes().size(),
                                                     MeasurementUnit.of( "DURATION" ),
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
        Map<MetricConstants, Duration> expectedSource = new HashMap<>();
        expectedSource.put( MetricConstants.MEAN, expectedMean );
        expectedSource.put( MetricConstants.MINIMUM, expectedMin );
        expectedSource.put( MetricConstants.MAXIMUM, expectedMax );
        expectedSource.put( MetricConstants.MEAN_ABSOLUTE, expectedMeanAbs );

        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        DurationScoreStatistic expected = DurationScoreStatistic.of( expectedSource, m1 );
        assertTrue( "Actual and expected results differ.", actual.equals( expected ) );
    }

    /**
     * Tests the {@link TimingErrorDurationStatistics#apply(TimeSeriesOfSingleValuedPairs)} and compares the actual
     * result to the expected result when the input contains no data.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testApplyWithNoData() throws MetricParameterException
    {
        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.MIN,
                                           Instant.MAX );

        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                        DatasetIdentifier.of( Location.of( "A" ),
                                                                                              "Streamflow" ),
                                                                        window ),
                                                     input.getBasisTimes().size(),
                                                     MeasurementUnit.of( "DURATION" ),
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

        Map<MetricConstants, Duration> expectedSource = new HashMap<>();
        expectedSource.put( MetricConstants.MEAN, MetricConstants.MissingValues.MISSING_DURATION );

        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        DurationScoreStatistic expected = DurationScoreStatistic.of( expectedSource, m1 );

        assertTrue( "Actual and expected results differ.", actual.equals( expected ) );
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for a missing statistic.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnNullStatistics() throws MetricParameterException
    {
        // Missing statistic
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify a non-null container of summary statistics." );

        //Build the metric
        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC, null );
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for an empty statistic.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnEmptyStatistics() throws MetricParameterException
    {
        // Empty statistic
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify one or more summary statistics." );

        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC, Collections.emptySet() );
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for a null statistic.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnNullStatistic() throws MetricParameterException
    {
        // Null statistic
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot build the metric with a null statistic." );

        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC, Collections.singleton( null ) );
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for an unrecognized statistic.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnUnrecognizedStatistic() throws MetricParameterException
    {
        // Unrecognized statistic
        exception.expect( IllegalArgumentException.class );

        TimingErrorDurationStatistics.of( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                          Collections.singleton( MetricConstants.NONE ) );
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for an unrecognized identifier.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnNullIdentifier() throws MetricParameterException
    {
        // Null identifier
        exception.expect( MetricParameterException.class );

        TimingErrorDurationStatistics.of( null,
                                          Collections.singleton( MetricConstants.MEAN ) );
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for null input when calling 
     * {@link TimingErrorDurationStatistics#apply(wres.datamodel.statistics.PairedOutput)}.
     * @throws MetricParameterException if the metric could not be constructed
     */

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
