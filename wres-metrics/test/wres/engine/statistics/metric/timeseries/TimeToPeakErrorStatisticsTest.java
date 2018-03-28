package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.timeseries.TimeToPeakErrorStatistics.TimeToPeakErrorStatisticBuilder;

/**
 * Tests the {@link TimeToPeakErrorStatistics}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public final class TimeToPeakErrorStatisticsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Tests the {@link TimeToPeakErrorStatistics#apply(TimeSeriesOfSingleValuedPairs)} and compares the actual result 
     * to the expected result when adding one summary statistic to each instance. 
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1ApplyOneStatisticPerInstance() throws MetricParameterException
    {
        // Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getBasisTimes().size(),
                                                                   metaFac.getDimension( "DURATION" ),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                   MetricConstants.MEAN,
                                                                   metaFac.getDatasetIdentifier( "A",
                                                                                                 "Streamflow" ),
                                                                   window );
        // Build the metric
        final TimeToPeakErrorStatisticBuilder b = new TimeToPeakErrorStatisticBuilder();
        b.setStatistics( Collections.singleton( MetricConstants.MEAN ) ).setOutputFactory( outF );
        final TimeToPeakErrorStatistics ttps = b.build();

        // Check the parameters
        assertTrue( "Unexpected name for the Time-to-Peak Error Statistic.",
                    ttps.getName().equals( MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC.toString() ) );
        assertFalse( "Time-to-Peak Error Statistic is not decomposable.", ttps.isDecomposable() );
        assertFalse( "Time-to-Peak Error Statistic is not a skill score.", ttps.isSkillScore() );
        assertTrue( "Time-to-Peak Error Statistic cannot be decomposed.",
                    ttps.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        assertTrue( "Time-to-Peak Error Statistic has real units.", ttps.hasRealUnits() );
        assertTrue( "Time-to-Peak Error Statistic is a collection of " + MetricConstants.TIME_TO_PEAK_ERROR,
                    MetricConstants.TIME_TO_PEAK_ERROR == ttps.getCollectionOf() );

        // Check the results
        final DurationScoreOutput actual = ttps.apply( input );
        final Duration expectedSource = Duration.ofHours( 3 );
        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        final DurationScoreOutput expected = outF.ofDurationScoreOutput( expectedSource, m1 );
        assertTrue( "Actual: " + actual.getComponent( MetricConstants.MEAN )
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );

        // Check some additional statistics
        // Maximum error = 12
        DurationScoreOutput max = b.setStatistics( Collections.singleton( MetricConstants.MAXIMUM ) )
                                   .build()
                                   .apply( input );
        assertTrue( "Actual: " + max.getComponent( MetricConstants.MAXIMUM ).getData()
                    + ". Expected: "
                    + Duration.ofHours( 12 )
                    + ".",
                    max.getComponent( MetricConstants.MAXIMUM ).getData().equals( Duration.ofHours( 12 ) ) );
        // Minimum error = -6
        DurationScoreOutput min = b.setStatistics( Collections.singleton( MetricConstants.MINIMUM ) )
                                   .build()
                                   .apply( input );
        assertTrue( "Actual: " + min.getComponent( MetricConstants.MINIMUM ).getData()
                    + ". Expected: "
                    + Duration.ofHours( -6 )
                    + ".",
                    min.getComponent( MetricConstants.MINIMUM ).getData().equals( Duration.ofHours( -6 ) ) );
        // Mean absolute error = 9
        DurationScoreOutput meanAbs = b.setStatistics( Collections.singleton( MetricConstants.MEAN_ABSOLUTE ) )
                                       .build()
                                       .apply( input );
        assertTrue( "Actual: " + meanAbs.getComponent( MetricConstants.MEAN_ABSOLUTE ).getData()
                    + ". Expected: "
                    + Duration.ofHours( 9 )
                    + ".",
                    meanAbs.getComponent( MetricConstants.MEAN_ABSOLUTE ).getData().equals( Duration.ofHours( 9 ) ) );
    }

    /**
     * Tests the {@link TimeToPeakErrorStatistics#apply(TimeSeriesOfSingleValuedPairs)} and compares the actual result 
     * to the expected result when adding multiple summary statistics to one instance. 
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test2ApplyMultipleStatisticInOneInstance() throws MetricParameterException
    {
        // Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getBasisTimes().size(),
                                                                   metaFac.getDimension( "DURATION" ),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC,
                                                                   null,
                                                                   metaFac.getDatasetIdentifier( "A",
                                                                                                 "Streamflow" ),
                                                                   window );
        // Build the metric
        final TimeToPeakErrorStatisticBuilder b = new TimeToPeakErrorStatisticBuilder();
        b.setStatistics( new HashSet<>( Arrays.asList( MetricConstants.MEAN,
                                                       MetricConstants.MAXIMUM,
                                                       MetricConstants.MINIMUM,
                                                       MetricConstants.MEAN_ABSOLUTE ) ) )
         .setOutputFactory( outF );
        final TimeToPeakErrorStatistics ttps = b.build();

        // Check the results
        final DurationScoreOutput actual = ttps.apply( input );
        final Duration expectedMean = Duration.ofHours( 3 );
        final Duration expectedMin = Duration.ofHours( -6 );
        final Duration expectedMax = Duration.ofHours( 12 );
        final Duration expectedMeanAbs = Duration.ofHours( 9 );
        Map<MetricConstants, Duration> expectedSource = new HashMap<>();
        expectedSource.put( MetricConstants.MEAN, expectedMean );
        expectedSource.put( MetricConstants.MINIMUM, expectedMin );
        expectedSource.put( MetricConstants.MAXIMUM, expectedMax );
        expectedSource.put( MetricConstants.MEAN_ABSOLUTE, expectedMeanAbs );
        // Expected, which uses identifier of MetricConstants.MAIN for convenience
        final DurationScoreOutput expected = outF.ofDurationScoreOutput( expectedSource, m1 );
        assertTrue( "Actual and expected results differ.", actual.equals( expected ) );
    }

    /**
     * Constructs a {@link TimeToPeakErrorStatistics} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test3ExceptionOnMissingStatistic() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final TimeToPeakErrorStatisticBuilder b =
                new TimeToPeakErrorStatisticBuilder();
        b.setOutputFactory( outF );

        // Missing statistic
        exception.expect( MetricParameterException.class );
        b.build();
    }

    @Test
    public void test3ExceptionOnEmptyStatistic() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final TimeToPeakErrorStatisticBuilder b = new TimeToPeakErrorStatisticBuilder();
        b.setOutputFactory( outF );

        // Empty statistic
        exception.expect( MetricParameterException.class );

        TimeToPeakErrorStatisticBuilder c = new TimeToPeakErrorStatisticBuilder();
        c.setOutputFactory( outF );
        c.setStatistics( Collections.emptySet() ).build();
    }

    @Test
    public void test3ExceptionOnNullStatistic() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final TimeToPeakErrorStatisticBuilder b = new TimeToPeakErrorStatisticBuilder();
        b.setOutputFactory( outF );

        // Null statistic
        exception.expect( MetricParameterException.class );

        TimeToPeakErrorStatisticBuilder c = new TimeToPeakErrorStatisticBuilder();
        c.setStatistics( Collections.singleton( null ) );
        c.build();
    }

    @Test
    public void test3ExceptionOnUnrecognizedStatistic() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final TimeToPeakErrorStatisticBuilder b = new TimeToPeakErrorStatisticBuilder();
        b.setOutputFactory( outF );

        // Unrecognized statistic
        exception.expect( IllegalArgumentException.class );

        TimeToPeakErrorStatisticBuilder c = new TimeToPeakErrorStatisticBuilder();
        c.setOutputFactory( outF );
        c.setStatistics( Collections.singleton( MetricConstants.NONE ) );
        c.build();
    }

    @Test
    public void test3ExceptionOnNullInputToApply() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final TimeToPeakErrorStatisticBuilder b = new TimeToPeakErrorStatisticBuilder();
        b.setOutputFactory( outF );

        // Null input to apply
        exception.expect( MetricInputException.class );

        TimeToPeakErrorStatisticBuilder c = new TimeToPeakErrorStatisticBuilder();
        c.setOutputFactory( outF );
        c.setStatistics( Collections.singleton( MetricConstants.MEAN ) );
        c.build().apply( null );
    }

    @Test
    public void test3ExceptionOnNullInputToAggregate() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final TimeToPeakErrorStatisticBuilder b = new TimeToPeakErrorStatisticBuilder();
        b.setOutputFactory( outF );

        // Null input to aggregate
        exception.expect( MetricInputException.class );

        TimeToPeakErrorStatisticBuilder c = new TimeToPeakErrorStatisticBuilder();
        c.setOutputFactory( outF );
        c.setStatistics( Collections.singleton( MetricConstants.MEAN ) );
        c.build().aggregate( null );
    }

}
