package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

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

    /**
     * Constructs a {@link TimeToPeakErrorStatistics} and compares the actual result to the expected result. Also, checks 
     * the parameters.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1TimeToPeakErrorStatistics() throws MetricParameterException
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
        b.setStatistic( MetricConstants.MEAN ).setOutputFactory( outF );
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
        assertTrue( "Actual: " + actual.getValue( MetricConstants.MEAN )
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );

        // Check some additional statistics
        // Maximum error = 12
        DurationScoreOutput max = b.setStatistic( MetricConstants.MAXIMUM )
                                   .build()
                                   .apply( input );
        assertTrue( "Actual: " + max.getValue( MetricConstants.MAXIMUM )
                    + ". Expected: "
                    + Duration.ofHours( 12 )
                    + ".",
                    max.getValue( MetricConstants.MAXIMUM ).equals( Duration.ofHours( 12 ) ) );
        // Minimum error = -6
        DurationScoreOutput min = b.setStatistic( MetricConstants.MINIMUM )
                                   .build()
                                   .apply( input );
        assertTrue( "Actual: " + min.getValue( MetricConstants.MINIMUM )
                    + ". Expected: "
                    + Duration.ofHours( -6 )
                    + ".",
                    min.getValue( MetricConstants.MINIMUM ).equals( Duration.ofHours( -6 ) ) );
        // Mean absolute error = 9
        DurationScoreOutput meanAbs = b.setStatistic( MetricConstants.MEAN_ABSOLUTE )
                                   .build()
                                   .apply( input );
        assertTrue( "Actual: " + meanAbs.getValue( MetricConstants.MEAN_ABSOLUTE )
                    + ". Expected: "
                    + Duration.ofHours( 9 )
                    + ".",
                    meanAbs.getValue( MetricConstants.MEAN_ABSOLUTE ).equals( Duration.ofHours( 9 ) ) );
    }

    /**
     * Constructs a {@link TimeToPeakErrorStatistics} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final TimeToPeakErrorStatisticBuilder b = new TimeToPeakErrorStatisticBuilder();
        b.setOutputFactory( outF );
        // Missing statistic
        try
        {
            b.build();
            fail( "Expected an exception on a missing statistic." );
        }
        catch ( MetricParameterException e )
        {
        }
        // Unrecognized statistic
        try
        {
            b.setStatistic( MetricConstants.NONE );
            b.build();
            fail( "Expected an exception on a missing statistic." );
        }
        catch ( MetricParameterException e )
        {
        }
        // Null input to apply
        try
        {
            b.setStatistic( MetricConstants.MEAN );
            b.build().apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
        // Null input to aggregate
        try
        {
            b.setStatistic( MetricConstants.MEAN );
            b.build().aggregate( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }

    }

}
