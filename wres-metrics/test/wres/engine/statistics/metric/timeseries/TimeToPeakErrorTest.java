package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DatasetIdentifier;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.Location;
import wres.datamodel.sampledata.MeasurementUnit;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.SampleMetadata.SampleMetadataBuilder;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
import wres.datamodel.statistics.PairedStatistic;
import wres.datamodel.statistics.StatisticMetadata;
import wres.datamodel.time.TimeWindow;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.SumOfSquareError;

/**
 * Tests the {@link TimeToPeakError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeToPeakErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link SumOfSquareError}.
     */

    private TimeToPeakError ttp;

    @Before
    public void setupBeforeEachTest()
    {
        ttp = TimeToPeakError.of( new Random( 123456789 ) );
    }

    @Test
    public void testTimeToPeakError()
    {
        // Generate some data
        PoolOfPairs<Double,Double> input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 18 ) );
        final TimeWindow timeWindow = window;

        StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( FeatureKey.of( "A" ),
                                                                                                       "Streamflow" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.get().size(),
                                      MeasurementUnit.of( "DURATION" ),
                                      MetricConstants.TIME_TO_PEAK_ERROR,
                                      MetricConstants.MAIN );

        // Check the parameters
        assertTrue( "Unexpected name for the Time-to-Peak Error.",
                    ttp.getName().equals( MetricConstants.TIME_TO_PEAK_ERROR.toString() ) );

        // Check the results
        PairedStatistic<Instant, Duration> actual = ttp.apply( input );
        List<Pair<Instant, Duration>> expectedSource = new ArrayList<>();
        expectedSource.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
        expectedSource.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );
        PairedStatistic<Instant, Duration> expected = PairedStatistic.of( expectedSource, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    @Test
    public void testHasRealUnitsReturnsTrue()
    {
        assertTrue( ttp.hasRealUnits() );
    }

    @Test
    public void testApplyThrowsExceptionOnNullInput()
    {
        //Check the exceptions
        exception.expect( SampleDataException.class );

        ttp.apply( null );
    }  

}
