package wres.engine.statistics.metric.timeseries;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

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
import wres.engine.statistics.metric.Incremental;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.timeseries.TimeToPeakError.TimeToPeakErrorBuilder;

/**
 * Tests the {@link TimeToPeakError}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.4
 */
public final class TimeToPeakErrorTest
{

    /**
     * Constructs a {@link TimeToPeakError} and compares the actual result to the expected result. Also, checks 
     * the parameters.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1TimeToPeakError() throws MetricParameterException
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
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A",
                                                                                                 "Streamflow" ),
                                                                   window );
        // Build the metric
        final TimeToPeakErrorBuilder b = new TimeToPeakErrorBuilder();
        b.setOutputFactory( outF );
        final TimeToPeakError ttp = b.build();

        // Check the parameters
        assertTrue( "Unexpected name for the Time-to-Peak Error.",
                    ttp.getName().equals( MetricConstants.TIME_TO_PEAK_ERROR.toString() ) );

        // Check the results
        final PairedOutput<Instant, Duration> actual = ttp.apply( input );
        List<Pair<Instant, Duration>> expectedSource = new ArrayList<>();
        expectedSource.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
        expectedSource.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );
        final PairedOutput<Instant, Duration> expected = outF.ofPairedOutput( expectedSource, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Tests the behavior of {@link Incremental} implemented by {@link TimeToPeakError}.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test2CombineAndComplete() throws MetricParameterException
    {
        // Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        // Generate some data
        TimeSeriesOfSingleValuedPairs input = MetricTestDataFactory.getTimeSeriesOfSingleValuedPairsOne();

        // Metadata for the output with sample size * 2
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "1985-01-02T00:00:00Z" ),
                                                 ReferenceTime.ISSUE_TIME,
                                                 Duration.ofHours( 6 ),
                                                 Duration.ofHours( 18 ) );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getBasisTimes().size() * 2,
                                                                   metaFac.getDimension( "DURATION" ),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.TIME_TO_PEAK_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "A",
                                                                                                 "Streamflow" ),
                                                                   window );
        // Build the metric
        final TimeToPeakErrorBuilder b = new TimeToPeakErrorBuilder();
        b.setOutputFactory( outF );
        final TimeToPeakError ttp = b.build();

        // Compute the combined and completed results
        final PairedOutput<Instant, Duration> actual = ttp.complete( ttp.combine( input, ttp.apply( input ) ) );

        List<Pair<Instant, Duration>> expectedSource = new ArrayList<>();
        expectedSource.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
        expectedSource.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );
        expectedSource.add( Pair.of( Instant.parse( "1985-01-01T00:00:00Z" ), Duration.ofHours( -6 ) ) );
        expectedSource.add( Pair.of( Instant.parse( "1985-01-02T00:00:00Z" ), Duration.ofHours( 12 ) ) );
        final PairedOutput<Instant, Duration> expected = outF.ofPairedOutput( expectedSource, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Constructs a {@link TimeToPeakError} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test3Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final TimeToPeakErrorBuilder b = new TimeToPeakErrorBuilder();
        b.setOutputFactory( outF );
        final TimeToPeakError ttp = b.build();

        //Check the exceptions
        try
        {
            ttp.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }

}
