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
import wres.datamodel.sampledata.DatasetIdentifier;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.TimeSeriesOfPairs;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeWindow;
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

    @Test
    public void testTimeToPeakRelativeError()
    {
        // Generate some data
        TimeSeriesOfPairs<Double,Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

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
                                      input.get().size(),
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

    @Test
    public void testApplyThrowsExceptionOnNullInput()
    {
        //Check the exceptions
        exception.expect( SampleDataException.class );

        TimeToPeakRelativeError ttp = TimeToPeakRelativeError.of();

        ttp.apply( null );
    }

}
