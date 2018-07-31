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
 * Tests the {@link VolumetricEfficiency}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class VolumetricEfficiencyTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link VolumetricEfficiency}.
     */

    private VolumetricEfficiency ve;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.ve = VolumetricEfficiency.of();
    }

    /**
     * Compares the output from {@link VolumetricEfficiency#apply(SingleValuedPairs)} against expected output.
     * @throws IOException if the input data could not be read
     */

    @Test
    public void testApply() throws IOException
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "MM/DAY" ),
                                                                 MetricConstants.VOLUMETRIC_EFFICIENCY,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( Location.of( "103.1" ),
                                                                                       "QME",
                                                                                       "NVE" ),
                                                                 window,
                                                                 null );
        //Check the results
        DoubleScoreOutput actual = ve.apply( input );
        DoubleScoreOutput expected = DoubleScoreOutput.of( 0.657420176533252, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link VolumetricEfficiency#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );

        DoubleScoreOutput actual = ve.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#getName()} returns 
     * {@link MetricConstants#VOLUMETRIC_EFFICIENCY.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( ve.getName().equals( MetricConstants.VOLUMETRIC_EFFICIENCY.toString() ) );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( ve.isDecomposable() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( ve.isSkillScore() );
    }

    /**
     * Checks that the {@link VolumetricEfficiency#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( ve.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link VolumetricEfficiency#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'VOLUMETRIC EFFICIENCY'." );

        ve.apply( null );
    }

}
