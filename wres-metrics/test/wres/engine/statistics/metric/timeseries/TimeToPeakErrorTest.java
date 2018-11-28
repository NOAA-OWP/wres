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
    public void setupBeforeEachTest() throws MetricParameterException
    {
        ttp = TimeToPeakError.of( new Random( 123456789 ) );
    }

    /**
     * Constructs a {@link TimeToPeakError} and compares the actual result to the expected result. Also, checks 
     * the parameters.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testTimeToPeakError() throws MetricParameterException
    {
        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "1985-01-02T00:00:00Z" ),
                                           Duration.ofHours( 6 ),
                                           Duration.ofHours( 18 ) );
        final TimeWindow timeWindow = window;

        StatisticMetadata m1 =
                StatisticMetadata.of( new SampleMetadataBuilder().setMeasurementUnit( MeasurementUnit.of( "CMS" ) )
                                                                 .setIdentifier( DatasetIdentifier.of( Location.of( "A" ),
                                                                                                       "Streamflow" ) )
                                                                 .setTimeWindow( timeWindow )
                                                                 .build(),
                                      input.getBasisTimes().size(),
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

    /**
     * Checks that the {@link TimeToPeakError#hasRealUnits()} returns <code>true</code>.
     */

    @Test
    public void testHasRealUnitsReturnsTrue()
    {
        assertTrue( ttp.hasRealUnits() );
    }

    /**
     * Checks that {@link TimeToPeakError#apply(TimeSeriesOfSingleValuedPairs)} throws an exception when 
     * provided with null input.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testApplyThrowsExceptionOnNullInput() throws MetricParameterException
    {
        //Check the exceptions
        exception.expect( SampleDataException.class );

        ttp.apply( null );
    }

//  /**
//  * Tests the behavior of {@link Incremental} implemented by {@link TimeToPeakError}.
//  * @throws MetricParameterException if the metric could not be constructed 
//  */
//
// @Test
// public void testCombineAndComplete() throws MetricParameterException
// {
//     // Obtain the factories
//     final DataFactory DataFactory = DefaultDataFactory.getInstance();
//     final MetadataFactory MetadataFactory = DataFactory.getMetadataFactory();
//
//     // Generate some data
//     TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();
//
//     // Metadata for the output with sample size * 2
//     final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
//                                              Instant.parse( "1985-01-02T00:00:00Z" ),
//                                              ReferenceTime.ISSUE_TIME,
//                                              Duration.ofHours( 6 ),
//                                              Duration.ofHours( 18 ) );
//     final MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( input.getBasisTimes().size() * 2,
//                                                                MetadataFactory.getDimension( "DURATION" ),
//                                                                MetadataFactory.getDimension( "CMS" ),
//                                                                MetricConstants.TIME_TO_PEAK_ERROR,
//                                                                MetricConstants.MAIN,
//                                                                MetadataFactory.getDatasetIdentifier( "A",
//                                                                                              "Streamflow" ),
//                                                                window );
//     // Build the metric
//     final TimeToPeakErrorBuilder b = new TimeToPeakErrorBuilder();
//     b.setOutputFactory( DataFactory );
//     final TimeToPeakError ttp = b.build();
//
//     // Compute the combined and completed results
//     final PairedOutput<Instant, Duration> actual = ttp.complete( ttp.combine( input, ttp.apply( input ) ) );
//
//     List<Pair<Instant, Duration>> expectedSource = new ArrayList<>();
//     expectedSource.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
//     expectedSource.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );
//     expectedSource.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
//     expectedSource.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );
//     final PairedOutput<Instant, Duration> expected = DataFactory.ofPairedOutput( expectedSource, m1 );
//     assertTrue( "Actual: " + actual.getData()
//                 + ". Expected: "
//                 + expected.getData()
//                 + ".",
//                 actual.equals( expected ) );
// }    

}
