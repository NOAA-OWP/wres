package wres.engine.statistics.metric.discreteprobability;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.DiscreteProbabilityPairs;
import wres.datamodel.statistics.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link BrierSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BrierSkillScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link BrierSkillScore}.
     */

    private BrierSkillScore brierSkillScore;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.brierSkillScore = BrierSkillScore.of();
    }

    /**
     * Compares the output from {@link BrierSkillScore#apply(DiscreteProbabilityPairs)} against expected output for a 
     * dataset with a supplied baseline.
     */

    @Test
    public void testApplyWithSuppliedBaseline()
    {
        // Generate some data
        DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        // Metadata for the output
        MetricOutputMetadata m1 =
                MetricOutputMetadata.of( input.getRawData().size(),
                                                   MeasurementUnit.of(),
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BRIER_SKILL_SCORE,
                                                   MetricConstants.MAIN,
                                                   DatasetIdentifier.of( Location.of( "DRRC2" ), "SQIN", "HEFS", "ESP" ) );

        // Check the results       
        final DoubleScoreOutput actual = brierSkillScore.apply( input );
        final DoubleScoreOutput expected = DoubleScoreOutput.of( 0.11363636363636376, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link BrierSkillScore#apply(DiscreteProbabilityPairs)} against expected output for a 
     * dataset with a climatological baseline.
     */

    @Test
    public void testApplyWithClimatologicalBaseline()
    {
        // Generate some data
        DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        // Metadata for the output
        MetricOutputMetadata m1 =
                MetricOutputMetadata.of( input.getRawData().size(),
                                                   MeasurementUnit.of(),
                                                   MeasurementUnit.of(),
                                                   MetricConstants.BRIER_SKILL_SCORE,
                                                   MetricConstants.MAIN,
                                                   DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                         "SQIN",
                                                                                         "HEFS" ) );

        // Check the results       
        final DoubleScoreOutput actual = brierSkillScore.apply( input );
        final DoubleScoreOutput expected = DoubleScoreOutput.of( -0.040000000000000036, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }


    /**
     * Validates the output from {@link BrierSkillScore#apply(DiscreteProbabilityPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                DiscreteProbabilityPairs.of( Arrays.asList(), Metadata.of() );

        DoubleScoreOutput actual = brierSkillScore.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link BrierSkillScore#getName()} returns {@link MetricConstants.BRIER_SKILL_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( brierSkillScore.getName().equals( MetricConstants.BRIER_SKILL_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link BrierSkillScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( brierSkillScore.isDecomposable() );
    }

    /**
     * Checks that the {@link BrierSkillScore#isSkillScore()} returns <code>true</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertTrue( brierSkillScore.isSkillScore() );
    }

    /**
     * Checks that the {@link BrierSkillScore#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( brierSkillScore.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Checks that the {@link BrierSkillScore#isProper()} returns <code>false</code>.
     */

    @Test
    public void testIsProper()
    {
        assertFalse( brierSkillScore.isProper() );
    }

    /**
     * Checks that the {@link BrierSkillScore#isStrictlyProper()} returns <code>false</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertFalse( brierSkillScore.isStrictlyProper() );
    }

    /**
     * Tests for an expected exception on calling {@link BrierSkillScore#apply(DiscreteProbabilityPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'BRIER SKILL SCORE'." );

        brierSkillScore.apply( null );
    }

    /**
     * Tests for {@link Double#NaN} output when supplying {@link BrierSkillScore#apply(DiscreteProbabilityPairs)} with 
     * a baseline whose input is {@link Double#NaN}.
     */

    @Test
    public void testApplyNaNOutputWithNaNBaseline()
    {
        assertTrue( "Expected NaN for a forecast with only non-occurrences as the baseline.",
                    Double.isNaN( brierSkillScore.apply( MetricTestDataFactory.getDiscreteProbabilityPairsFour() )
                                                 .getData() ) );
    }
}
