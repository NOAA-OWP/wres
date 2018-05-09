package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilityScore.CRPSBuilder;

/**
 * Tests the {@link ContinuousRankedProbabilityScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class ContinousRankedProbabilityScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link ContinuousRankedProbabilityScore}.
     */

    private ContinuousRankedProbabilityScore crps;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        CRPSBuilder b = new CRPSBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        b.setDecompositionID( ScoreOutputGroup.NONE );
        this.crps = b.build();
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(EnsemblePairs)} against expected output
     * where the input contains no missing data.
     */

    @Test
    public void testApplyWithNoMissings()
    {
        //Generate some data
        MetadataFactory metaFac = outF.getMetadataFactory();
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 25.7, new double[] { 23, 43, 45, 23, 54 } ) );
        pairs.add( outF.pairOf( 21.4, new double[] { 19, 16, 57, 23, 9 } ) );
        pairs.add( outF.pairOf( 32.1, new double[] { 23, 54, 23, 12, 32 } ) );
        pairs.add( outF.pairOf( 47, new double[] { 12, 54, 23, 54, 78 } ) );
        pairs.add( outF.pairOf( 12.1, new double[] { 9, 8, 5, 6, 12 } ) );
        pairs.add( outF.pairOf( 43, new double[] { 23, 12, 12, 34, 10 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, metaFac.getMetadata() );

        //Metadata for the output
        MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getRawData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.MAIN );
        //Check the results       
        final DoubleScoreOutput actual = crps.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 7.63, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(EnsemblePairs)} against expected output
     * where the input contains missing data.
     */

    @Test
    public void testApplyWithMissings()
    {

        //Generate some data
        MetadataFactory metaFac = outF.getMetadataFactory();
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 25.7, new double[] { 23, 43, 45, 34.2, 23, 54 } ) );
        pairs.add( outF.pairOf( 21.4, new double[] { 19, 16, 57, 23, 9 } ) );
        pairs.add( outF.pairOf( 32.1, new double[] { 23, 54, 23, 12, 32, 45.3, 67.1 } ) );
        pairs.add( outF.pairOf( 47, new double[] { 12, 54, 23, 54 } ) );
        pairs.add( outF.pairOf( 12, new double[] { 9, 8, 5 } ) );
        pairs.add( outF.pairOf( 43, new double[] { 23, 12, 12 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, metaFac.getMetadata() );

        //Metadata for the output
        MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getRawData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.MAIN );
        //Check the results       
        DoubleScoreOutput actual = crps.apply( input );
        DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 8.734401927437641, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );

    }

    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(EnsemblePairs)} against expected output
     * where the observation falls below the lowest member.
     */

    @Test
    public void testApplyObsMissesLow()
    {
        //Generate some data
        MetadataFactory metaFac = outF.getMetadataFactory();
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 8, new double[] { 23, 54, 23, 12, 32 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, metaFac.getMetadata() );

        //Metadata for the output
        MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getRawData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.MAIN );
        //Check the results       
        DoubleScoreOutput actual = crps.apply( input );
        DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 13.36, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }
    
    /**
     * Compares the output from {@link ContinuousRankedProbabilityScore#apply(EnsemblePairs)} against expected output
     * for a scenario where the observed value overlaps one ensemble member. This exposes a mistake in the Hersbach 
     * (2000) paper where rows 1 and 3 of table/eqn. 26 should be inclusive bounds.
     */

    @Test
    public void testApplyObsEqualsMember()
    {

        //Generate some data
        MetadataFactory metaFac = outF.getMetadataFactory();
        List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 32, new double[] { 23, 54, 23, 12, 32 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, metaFac.getMetadata() );

        //Metadata for the output
        MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getRawData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.MAIN );
        //Check the results       
        DoubleScoreOutput actual = crps.apply( input );
        DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 4.56, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }    
    

    /**
     * Validates the output from {@link ContinuousRankedProbabilityScore#apply(EnsemblePairs)} when supplied with no 
     * data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        EnsemblePairs input =
                outF.ofEnsemblePairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );

        DoubleScoreOutput actual = crps.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#getName()} returns 
     * {@link MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( crps.getName().equals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( crps.isDecomposable() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( crps.isSkillScore() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#getScoreOutputGroup()} returns the result provided on 
     * construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( crps.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isProper()} returns <code>true</code>.
     */

    @Test
    public void testIsProper()
    {
        assertTrue( crps.isProper() );
    }

    /**
     * Checks that the {@link ContinuousRankedProbabilityScore#isStrictlyProper()} returns <code>true</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertTrue( crps.isStrictlyProper() );
    }

    /**
     * Tests for an expected exception on calling {@link ContinuousRankedProbabilityScore#apply(EnsemblePairs)} with 
     * null input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'CONTINUOUS RANKED PROBABILITY SCORE'." );

        crps.apply( null );
    }
    
    /**
     * Tests for an expected exception on building a {@link ContinuousRankedProbabilityScore} with 
     * an unrecognized decomposition identifier.
     * @throws MetricParameterException if the metric could not be built for an unexpected reason
     */

    @Test
    public void testApplyExceptionOnUnrecognizedDecompositionIdentifier() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Unsupported decomposition identifier 'LBR'." );
        CRPSBuilder b = new CRPSBuilder();
        b.setOutputFactory( outF );
        b.setDecompositionID( ScoreOutputGroup.LBR );
        b.build();
    }

}
