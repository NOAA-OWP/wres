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

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link KlingGuptaEfficiency}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class KlingGuptaEfficiencyTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link KlingGuptaEfficiency}.
     */

    private KlingGuptaEfficiency kge;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.kge = KlingGuptaEfficiency.of();
    }

    /**
     * Compares the output from {@link KlingGuptaEfficiency#apply(SingleValuedPairs)} against expected output.
     * @throws IOException if the input data could not be read
     */

    @Test
    public void testApply() throws IOException
    {
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 24 ) );
        final TimeWindow timeWindow = window;

        MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
        MeasurementUnit.of(),
        MeasurementUnit.of( "MM/DAY" ),
        MetricConstants.KLING_GUPTA_EFFICIENCY,
        MetricConstants.MAIN,
        DatasetIdentifier.of( Location.of("103.1"),
                                                                                                   "QME",
                                                                                                   "NVE" ),
        timeWindow );

        //Check the results
        DoubleScoreOutput actual = kge.apply( input );

        DoubleScoreOutput expected = DoubleScoreOutput.of( 0.8921704394462281, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link KlingGuptaEfficiency#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApplyTwo()
    {
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                             MeasurementUnit.of(),
                                                             MeasurementUnit.of(),
                                                             MetricConstants.KLING_GUPTA_EFFICIENCY,
                                                             MetricConstants.MAIN );

        //Check the results
        DoubleScoreOutput actual = kge.apply( input );

        DoubleScoreOutput expected = DoubleScoreOutput.of( 0.9432025316651065, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link KlingGuptaEfficiency#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );

        DoubleScoreOutput actual = kge.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link KlingGuptaEfficiency#getName()} returns
     * {@link MetricConstants#KLING_GUPTA_EFFICIENCY.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( kge.getName().equals( MetricConstants.KLING_GUPTA_EFFICIENCY.toString() ) );
    }

    /**
     * Checks that the {@link KlingGuptaEfficiency#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( kge.isDecomposable() );
    }

    /**
     * Checks that the {@link KlingGuptaEfficiency#isSkillScore()} returns <code>true</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertTrue( kge.isSkillScore() );
    }

    /**
     * Checks that the {@link KlingGuptaEfficiency#hasRealUnits()} returns <code>false</code>.
     */

    @Test
    public void testhasRealUnits()
    {
        assertFalse( kge.hasRealUnits() );
    }

    /**
     * Checks that the {@link KlingGuptaEfficiency#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( kge.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link KlingGuptaEfficiency#apply(SingleValuedPairs)} with null
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'KLING GUPTA EFFICIENCY'." );

        kge.apply( null );
    }

}
