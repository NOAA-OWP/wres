package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link IndexOfAgreement}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class IndexOfAgreementTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link IndexOfAgreement}.
     */

    private IndexOfAgreement ioa;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.ioa = IndexOfAgreement.of();
    }

    /**
     * Compares the output from {@link IndexOfAgreement#apply(SingleValuedPairs)} against expected output.
     * @throws IOException if the input data could not be read
     */

    @Test
    public void testApply() throws IOException
    {
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "MM/DAY" ),
                                                                 MetricConstants.INDEX_OF_AGREEMENT,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( Location.of( "103.1" ),
                                                                                       "QME",
                                                                                       "NVE" ),
                                                                 window,
                                                                 null,
                                                                 null  );

        //Check the results
        DoubleScoreOutput actual = ioa.apply( input );

        DoubleScoreOutput expected = DoubleScoreOutput.of( 0.8221179993380173, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link IndexOfAgreement#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );

        DoubleScoreOutput actual = ioa.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link IndexOfAgreement#getName()} returns
     * {@link MetricConstants#INDEX_OF_AGREEMENT.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( ioa.getName().equals( MetricConstants.INDEX_OF_AGREEMENT.toString() ) );
    }

    /**
     * Checks that the {@link IndexOfAgreement#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( ioa.isDecomposable() );
    }

    /**
     * Checks that the {@link IndexOfAgreement#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( ioa.isSkillScore() );
    }

    /**
     * Checks that the {@link IndexOfAgreement#hasRealUnits()} returns <code>false</code>.
     */

    @Test
    public void testhasRealUnits()
    {
        assertFalse( ioa.hasRealUnits() );
    }

    /**
     * Checks that the {@link IndexOfAgreement#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( ioa.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link IndexOfAgreement#apply(SingleValuedPairs)} with null
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'INDEX OF AGREEMENT'." );

        ioa.apply( null );
    }

}
