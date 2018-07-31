package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.EnsemblePair;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link ContinuousRankedProbabilitySkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContinousRankedProbabilitySkillScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link ContinuousRankedProbabilitySkillScore}.
     */

    private ContinuousRankedProbabilitySkillScore crpss;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.crpss = ContinuousRankedProbabilitySkillScore.of();
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilitySkillScore#apply(EnsemblePairs)} against expected 
     * output for a dataset with a supplied baseline.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        List<EnsemblePair> pairs = new ArrayList<>();
        pairs.add( EnsemblePair.of( 25.7, new double[] { 23, 43, 45, 23, 54 } ) );
        pairs.add( EnsemblePair.of( 21.4, new double[] { 19, 16, 57, 23, 9 } ) );
        pairs.add( EnsemblePair.of( 32.1, new double[] { 23, 54, 23, 12, 32 } ) );
        pairs.add( EnsemblePair.of( 47, new double[] { 12, 54, 23, 54, 78 } ) );
        pairs.add( EnsemblePair.of( 12, new double[] { 9, 8, 5, 6, 12 } ) );
        pairs.add( EnsemblePair.of( 43, new double[] { 23, 12, 12, 34, 10 } ) );
        List<EnsemblePair> basePairs = new ArrayList<>();
        basePairs.add( EnsemblePair.of( 25.7, new double[] { 20, 43, 45, 23, 94 } ) );
        basePairs.add( EnsemblePair.of( 21.4, new double[] { 19, 76, 57, 23, 9 } ) );
        basePairs.add( EnsemblePair.of( 32.1, new double[] { 23, 53, 23, 12, 32 } ) );
        basePairs.add( EnsemblePair.of( 47, new double[] { 2, 54, 23, 54, 78 } ) );
        basePairs.add( EnsemblePair.of( 12.1, new double[] { 9, 18, 5, 6, 12 } ) );
        basePairs.add( EnsemblePair.of( 43, new double[] { 23, 12, 12, 39, 10 } ) );
        EnsemblePairs input = EnsemblePairs.of( pairs,
                                                           basePairs,
                                                           Metadata.of(),
                                                           Metadata.of() );

        //Metadata for the output
        MetricOutputMetadata m1 =
                MetricOutputMetadata.of( input.getRawData().size(),
                                                   MeasurementUnit.of(),
                                                   MeasurementUnit.of(),
                                                   MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                   MetricConstants.MAIN );
        //Check the results       
        DoubleScoreOutput actual = crpss.apply( input );
        DoubleScoreOutput expected = DoubleScoreOutput.of( 0.0779168348809044, m1 );

        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link ContinuousRankedProbabilitySkillScore#apply(EnsemblePairs)} when supplied 
     * with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        EnsemblePairs input =
                EnsemblePairs.of( Arrays.asList(),
                                             Arrays.asList(),
                                             Metadata.of(),
                                             Metadata.of() );

        DoubleScoreOutput actual = crpss.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#getName()} returns 
     * {@link MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( crpss.getName().equals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( crpss.isDecomposable() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#isSkillScore()} returns <code>true</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertTrue( crpss.isSkillScore() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#getScoreOutputGroup()} returns the result 
     * provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( crpss.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#isProper()} returns <code>false</code>.
     */

    @Test
    public void testIsProper()
    {
        assertFalse( crpss.isProper() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilitySkillScore#isStrictlyProper()} returns <code>false</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertFalse( crpss.isStrictlyProper() );
    }

    /**
     * Checks that the baseline identifier is correctly propagated to the metric output metadata.
     * @throws IOException if the input pairs could not be read
     */

    @Test
    public void testMetadataContainsBaselineIdentifier() throws IOException
    {
        EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();

        assertTrue( crpss.apply( pairs ).getMetadata().getIdentifier().getScenarioIDForBaseline().equals( "ESP" ) );
    }

    /**
     * Tests for an expected exception on calling {@link ContinuousRankedProbabilitySkillScore#apply(EnsemblePairs)} 
     * with null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'CONTINUOUS RANKED PROBABILITY SKILL SCORE'." );

        crpss.apply( null );
    }

    /**
     * Tests for an expected exception on building a {@link ContinuousRankedProbabilitySkillScore} with 
     * an unrecognized decomposition identifier.
     * @throws MetricParameterException if the metric could not be built for an unexpected reason
     */

    @Test
    public void testApplyExceptionOnUnrecognizedDecompositionIdentifier() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Unsupported decomposition identifier 'LBR'." );
        
        ContinuousRankedProbabilitySkillScore.of( ScoreOutputGroup.LBR );
    }

    /**
     * Checks for an expected exception when the input data does not contain an explicit baseline.
     */

    @Test
    public void testExceptionOnInputWithMissingBaseline()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify a non-null baseline for the 'CONTINUOUS RANKED PROBABILITY SKILL SCORE'." );
        List<EnsemblePair> pairs = new ArrayList<>();
        pairs.add( EnsemblePair.of( 25.7, new double[] { 23, 43, 45, 23, 54 } ) );
        EnsemblePairs input = EnsemblePairs.of( pairs, Metadata.of() );
        crpss.apply( input );
    }


}
