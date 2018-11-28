package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.statistics.PairedStatistic;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link TimeToPeakRelativeError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeToPeakRelativeErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Constructs a {@link TimeToPeakRelativeError} and compares the actual result to the expected result. Also, checks 
     * the parameters.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testTimeToPeakRelativeError() throws MetricParameterException
    {
        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );

        StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                       "Streamflow" ) )
                                                                 .setTimeWindow( window )
                                                                 .build(),
                                      input.getBasisTimes().size(),
                                      MeasurementUnit.of( "DURATION IN RELATIVE HOURS" ),
                                      MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR,
                                      MetricConstants.MAIN );
        // Build the metric
        TimeToPeakRelativeError ttp = TimeToPeakRelativeError.of();

        // Check the parameters
        assertTrue( "Unexpected name for the Time-to-Peak Relative Error.",
                    ttp.getName().equals( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR.toString() ) );

        // Check the results
        final PairedStatistic<Instant, Duration> actual = ttp.apply( input );
        List<Pair<Instant, Duration>> expectedSource = new ArrayList<>();
        expectedSource.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofMinutes( -20 ) ) );
        expectedSource.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 2 ) ) );
        final PairedStatistic<Instant, Duration> expected = PairedStatistic.of( expectedSource, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Checks that {@link TimeToPeakRelativeError#apply(TimeSeriesOfSingleValuedPairs)} throws an exception when 
     * provided with null input.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        //Check the exceptions
        exception.expect( SampleDataException.class );

        TimeToPeakRelativeError ttp = TimeToPeakRelativeError.of();

        ttp.apply( null );
    }

}
