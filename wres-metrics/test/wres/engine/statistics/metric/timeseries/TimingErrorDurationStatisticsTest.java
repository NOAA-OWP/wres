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

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.timeseries.TimeToPeakError.TimeToPeakErrorBuilder;
import wres.engine.statistics.metric.timeseries.TimingErrorDurationStatistics.TimingErrorDurationStatisticsBuilder;

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
        // Obtain the factories
        DataFactory outF = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outF.getMetadataFactory();

        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           ReferenceTime.ISSUE_TIME,
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 18 ) );
        MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getBasisTimes().size(),
                                                             metaFac.getDimension( "DURATION" ),
                                                             metaFac.getDimension( "CMS" ),
                                                             MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                             MetricConstants.MEAN,
                                                             metaFac.getDatasetIdentifier( "A",
                                                                                           "Streamflow" ),
                                                             window );
        // Build a metric
        TimeToPeakErrorBuilder peakErrorBuilder = new TimeToPeakErrorBuilder();
        peakErrorBuilder.setOutputFactory( outF );
        TimeToPeakError peakError = peakErrorBuilder.build();

        // Build the summary statistics
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        b.setStatistics( Collections.singleton( MetricConstants.MEAN ) )
         .setOutputFactory( outF )
         .setID( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC );
        TimingErrorDurationStatistics ttps = b.build();

        // Check the results
        DurationScoreOutput actual = ttps.apply( peakError.apply( input ) );
        Duration expectedSource = Duration.ofHours( 3 );
        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        DurationScoreOutput expected = outF.ofDurationScoreOutput( expectedSource, m1 );
        assertTrue( "Actual: " + actual.getComponent( MetricConstants.MEAN )
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );

        // Check some additional statistics
        // Maximum error = 12
        DurationScoreOutput max = b.setStatistics( Collections.singleton( MetricConstants.MAXIMUM ) )
                                   .build()
                                   .apply( peakError.apply( input ) );
        assertTrue( "Actual: " + max.getComponent( MetricConstants.MAXIMUM ).getData()
                    + ". Expected: "
                    + Duration.ofHours( 12 )
                    + ".",
                    max.getComponent( MetricConstants.MAXIMUM ).getData().equals( Duration.ofHours( 12 ) ) );
        // Minimum error = -6
        DurationScoreOutput min = b.setStatistics( Collections.singleton( MetricConstants.MINIMUM ) )
                                   .build()
                                   .apply( peakError.apply( input ) );
        assertTrue( "Actual: " + min.getComponent( MetricConstants.MINIMUM ).getData()
                    + ". Expected: "
                    + Duration.ofHours( -6 )
                    + ".",
                    min.getComponent( MetricConstants.MINIMUM ).getData().equals( Duration.ofHours( -6 ) ) );
        // Mean absolute error = 9
        DurationScoreOutput meanAbs = b.setStatistics( Collections.singleton( MetricConstants.MEAN_ABSOLUTE ) )
                                       .build()
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
        // Obtain the factories
        DataFactory outF = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outF.getMetadataFactory();

        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           ReferenceTime.ISSUE_TIME,
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 18 ) );
        MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getBasisTimes().size(),
                                                             metaFac.getDimension( "DURATION" ),
                                                             metaFac.getDimension( "CMS" ),
                                                             MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                             null,
                                                             metaFac.getDatasetIdentifier( "A",
                                                                                           "Streamflow" ),
                                                             window );

        // Build a metric
        TimeToPeakErrorBuilder peakErrorBuilder = new TimeToPeakErrorBuilder();
        peakErrorBuilder.setOutputFactory( outF );
        TimeToPeakError peakError = peakErrorBuilder.build();

        // Build the summary statistics
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        TimingErrorDurationStatistics ttps = b.setStatistics( new HashSet<>( Arrays.asList( MetricConstants.MEAN,
                                                                                            MetricConstants.MAXIMUM,
                                                                                            MetricConstants.MINIMUM,
                                                                                            MetricConstants.MEAN_ABSOLUTE ) ) )
                                              .setOutputFactory( outF )
                                              .setID( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC )
                                              .build();

        // Check the results
        DurationScoreOutput actual = ttps.apply( peakError.apply( input ) );
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
        DurationScoreOutput expected = outF.ofDurationScoreOutput( expectedSource, m1 );
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
        // Obtain the factories
        DataFactory outF = DefaultDataFactory.getInstance();
        MetadataFactory metaFac = outF.getMetadataFactory();

        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsFour();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.MIN,
                                                 Instant.MAX );
        MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getBasisTimes().size(),
                                                             metaFac.getDimension( "DURATION" ),
                                                             metaFac.getDimension( "CMS" ),
                                                             MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                             MetricConstants.MEAN,
                                                             metaFac.getDatasetIdentifier( "A",
                                                                                           "Streamflow" ),
                                                             window );

        // Build a metric
        TimeToPeakErrorBuilder peakErrorBuilder = new TimeToPeakErrorBuilder();
        peakErrorBuilder.setOutputFactory( outF );
        TimeToPeakError peakError = peakErrorBuilder.build();

        // Build the summary statistics
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        TimingErrorDurationStatistics ttps = b.setStatistics( new HashSet<>( Arrays.asList( MetricConstants.MEAN ) ) )
                                              .setOutputFactory( outF )
                                              .setID( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC )
                                              .build();

        // Check the results
        DurationScoreOutput actual = ttps.apply( peakError.apply( input ) );

        Map<MetricConstants, Duration> expectedSource = new HashMap<>();
        expectedSource.put( MetricConstants.MEAN, MetricConstants.MissingValues.MISSING_DURATION );

        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        DurationScoreOutput expected = outF.ofDurationScoreOutput( expectedSource, m1 );

        assertTrue( "Actual and expected results differ.", actual.equals( expected ) );
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for a missing statistic.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnMissingStatistic() throws MetricParameterException
    {
        //Build the metric
        DataFactory outF = DefaultDataFactory.getInstance();
        TimingErrorDurationStatisticsBuilder b =
                new TimingErrorDurationStatisticsBuilder();
        b.setOutputFactory( outF );

        // Missing statistic
        exception.expect( MetricParameterException.class );
        b.build();
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for an empty statistic.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnEmptyStatistic() throws MetricParameterException
    {
        //Build the metric
        DataFactory outF = DefaultDataFactory.getInstance();
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        b.setOutputFactory( outF );

        // Empty statistic
        exception.expect( MetricParameterException.class );

        TimingErrorDurationStatisticsBuilder c = new TimingErrorDurationStatisticsBuilder();
        c.setOutputFactory( outF );
        c.setStatistics( Collections.emptySet() ).build();
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for a null statistic.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnNullStatistic() throws MetricParameterException
    {
        //Build the metric
        DataFactory outF = DefaultDataFactory.getInstance();
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        b.setOutputFactory( outF );

        // Null statistic
        exception.expect( MetricParameterException.class );

        TimingErrorDurationStatisticsBuilder c = new TimingErrorDurationStatisticsBuilder();
        c.setStatistics( Collections.singleton( null ) );
        c.build();
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for an unrecognized statistic.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnUnrecognizedStatistic() throws MetricParameterException
    {
        //Build the metric
        DataFactory outF = DefaultDataFactory.getInstance();
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        b.setOutputFactory( outF );

        // Unrecognized statistic
        exception.expect( IllegalArgumentException.class );

        TimingErrorDurationStatisticsBuilder c = new TimingErrorDurationStatisticsBuilder();
        c.setOutputFactory( outF )
         .setID( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC )
         .setStatistics( Collections.singleton( MetricConstants.NONE ) )
         .build();
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for an unrecognized identifier.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnNullIdentifier() throws MetricParameterException
    {
        //Build the metric
        DataFactory outF = DefaultDataFactory.getInstance();
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        b.setOutputFactory( outF );

        // Null identifier
        exception.expect( MetricParameterException.class );

        TimingErrorDurationStatisticsBuilder c = new TimingErrorDurationStatisticsBuilder();
        c.setOutputFactory( outF )
         .setStatistics( Collections.singleton( MetricConstants.MEAN ) )
         .build();
    }

    /**
     * Constructs a {@link TimingErrorDurationStatistics} and checks for null input when calling 
     * {@link TimingErrorDurationStatistics#apply(wres.datamodel.outputs.PairedOutput)}.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        //Build the metric
        DataFactory outF = DefaultDataFactory.getInstance();
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        b.setOutputFactory( outF );

        // Null input to apply
        exception.expect( MetricInputException.class );

        TimingErrorDurationStatisticsBuilder c = new TimingErrorDurationStatisticsBuilder();
        c.setOutputFactory( outF )
         .setID( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC )
         .setStatistics( Collections.singleton( MetricConstants.MEAN ) );
        c.build().apply( null );
    }

    /**
     * Tests for an expected exception on attempting to build a {@link TimingErrorDurationStatistics} with a null
     * builder.
     * @throws MetricParameterException if the test fails unexpectedly
     */

    @Test
    public void testBuildThrowsExceptionOnNullBuilder() throws MetricParameterException
    {
        // Null input to apply
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot construct the metric with a null builder." );

        new TimingErrorDurationStatistics( null );
    }

    /**
     * Tests for an expected exception on attempting to build a {@link TimingErrorDurationStatistics} with an empty
     * set of statistics.
     * @throws MetricParameterException if the test fails unexpectedly
     */

    @Test
    public void testBuildThrowsExceptionOnEmptyStatistics() throws MetricParameterException
    {
        // Null input to apply
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify one or more summary statistics." );

        DataFactory outF = DefaultDataFactory.getInstance();
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        b.setOutputFactory( outF );

        TimingErrorDurationStatisticsBuilder c = new TimingErrorDurationStatisticsBuilder();
        c.setOutputFactory( outF )
         .setID( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC )
         .setStatistics( new HashSet<>() );
        c.build();
    }

    /**
     * Tests for an expected exception on attempting to build a {@link TimingErrorDurationStatistics} with a set of
     * statistics that contains a null.
     * @throws MetricParameterException if the test fails unexpectedly
     */

    @Test
    public void testBuildThrowsExceptionOnNullStatistic() throws MetricParameterException
    {
        // Null input to apply
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot build the metric with a null statistic." );

        DataFactory outF = DefaultDataFactory.getInstance();
        TimingErrorDurationStatisticsBuilder b = new TimingErrorDurationStatisticsBuilder();
        b.setOutputFactory( outF );

        TimingErrorDurationStatisticsBuilder c = new TimingErrorDurationStatisticsBuilder();
        c.setOutputFactory( outF )
         .setID( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC )
         .setStatistics( Collections.singleton( null ) );
        c.build();
    }
}
