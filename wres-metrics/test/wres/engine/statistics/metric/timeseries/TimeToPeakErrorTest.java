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

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.PairedOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.SumOfSquareError;
import wres.engine.statistics.metric.timeseries.TimeToPeakError.TimeToPeakErrorBuilder;

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

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        outF = DefaultDataFactory.getInstance();
        TimeToPeakErrorBuilder b = new TimeToPeakErrorBuilder();
        b.setOutputFactory( outF );
        
        // Seeded RNG to resolve ties consistently
        Random resolveTies = new Random( 123456789 );
        b.setRNGForTies( resolveTies );
        
        ttp = b.build();
    }
    
    /**
     * Constructs a {@link TimeToPeakError} and compares the actual result to the expected result. Also, checks 
     * the parameters.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testTimeToPeakError() throws MetricParameterException
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
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A",
                                                                                                 "Streamflow" ),
                                                                   window );

        // Check the parameters
        assertTrue( "Unexpected name for the Time-to-Peak Error.",
                    ttp.getName().equals( MetricConstants.TIME_TO_PEAK_ERROR.toString() ) );

        // Check the results
        PairedOutput<Instant, Duration> actual = ttp.apply( input );
        List<Pair<Instant, Duration>> expectedSource = new ArrayList<>();
        expectedSource.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
        expectedSource.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );
        PairedOutput<Instant, Duration> expected = outF.ofPairedOutput( expectedSource, m1 );
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
        exception.expect( MetricInputException.class );

        ttp.apply( null );
    }
    
    /**
     * Tests for an expected exception on attempting to build a {@link TimeToPeakError} with a null 
     * builder.
     * @throws MetricParameterException if the test fails unexpectedly
     */

    @Test
    public void testBuildThrowsExceptionOnNullBuilder() throws MetricParameterException
    {
        // Null input to apply
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot construct the metric with a null builder." );

        new TimeToPeakError( null );
    }
    
    /**
     * Tests for an expected exception on attempting to build a {@link TimeToPeakError} with a null 
     * data factory.
     * @throws MetricParameterException if the test fails unexpectedly
     */

    @Test
    public void testBuildThrowsExceptionOnNullDataFactory() throws MetricParameterException
    {
        // Null input to apply
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify a data factory with which to build the metric." );

        new TimeToPeakError( new TimeToPeakErrorBuilder() );
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
//     final DataFactory outF = DefaultDataFactory.getInstance();
//     final MetadataFactory metaFac = outF.getMetadataFactory();
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
//     final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getBasisTimes().size() * 2,
//                                                                metaFac.getDimension( "DURATION" ),
//                                                                metaFac.getDimension( "CMS" ),
//                                                                MetricConstants.TIME_TO_PEAK_ERROR,
//                                                                MetricConstants.MAIN,
//                                                                metaFac.getDatasetIdentifier( "A",
//                                                                                              "Streamflow" ),
//                                                                window );
//     // Build the metric
//     final TimeToPeakErrorBuilder b = new TimeToPeakErrorBuilder();
//     b.setOutputFactory( outF );
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
//     final PairedOutput<Instant, Duration> expected = outF.ofPairedOutput( expectedSource, m1 );
//     assertTrue( "Actual: " + actual.getData()
//                 + ". Expected: "
//                 + expected.getData()
//                 + ".",
//                 actual.equals( expected ) );
// }    

}
