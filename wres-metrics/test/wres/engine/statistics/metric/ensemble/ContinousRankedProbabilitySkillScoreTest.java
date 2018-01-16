package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.MultiValuedScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilitySkillScore;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilitySkillScore.CRPSSBuilder;

/**
 * Tests the {@link ContinuousRankedProbabilitySkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ContinousRankedProbabilitySkillScoreTest
{

    /**
     * Constructs a {@link ContinuousRankedProbabilitySkillScore} and compares the actual result to the expected result 
     * for a scenario without  missing data. Also, checks the parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1CRPSSNoMissings() throws MetricParameterException
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 25.7, new double[] { 23, 43, 45, 23, 54 } ) );
        pairs.add( outF.pairOf( 21.4, new double[] { 19, 16, 57, 23, 9 } ) );
        pairs.add( outF.pairOf( 32.1, new double[] { 23, 54, 23, 12, 32 } ) );
        pairs.add( outF.pairOf( 47, new double[] { 12, 54, 23, 54, 78 } ) );
        pairs.add( outF.pairOf( 12, new double[] { 9, 8, 5, 6, 12 } ) );
        pairs.add( outF.pairOf( 43, new double[] { 23, 12, 12, 34, 10 } ) );
        final List<PairOfDoubleAndVectorOfDoubles> basePairs = new ArrayList<>();
        basePairs.add( outF.pairOf( 25.7, new double[] { 20, 43, 45, 23, 94 } ) );
        basePairs.add( outF.pairOf( 21.4, new double[] { 19, 76, 57, 23, 9 } ) );
        basePairs.add( outF.pairOf( 32.1, new double[] { 23, 53, 23, 12, 32 } ) );
        basePairs.add( outF.pairOf( 47, new double[] { 2, 54, 23, 54, 78 } ) );
        basePairs.add( outF.pairOf( 12.1, new double[] { 9, 18, 5, 6, 12 } ) );
        basePairs.add( outF.pairOf( 43, new double[] { 23, 12, 12, 39, 10 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, basePairs, metaFac.getMetadata(), metaFac.getMetadata() );

        //Build the metric
        final CRPSSBuilder b = new CRPSSBuilder();

        b.setDecompositionID( ScoreOutputGroup.NONE ).setOutputFactory( outF );

        final ContinuousRankedProbabilitySkillScore crpss = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                           MetricConstants.NONE );
        //Check the results       
        final MultiValuedScoreOutput actual = crpss.apply( input );
        final MultiValuedScoreOutput expected = outF.ofMultiValuedScoreOutput( new double[] { 0.0779168348809044 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );
        //Check the parameters
        assertTrue( "Unexpected name for the Continous Ranked Probability Skill Score.",
                    crpss.getName()
                         .equals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE.toString() ) );
        assertTrue( "The Continuous Ranked Probability Skill Score is decomposable.", crpss.isDecomposable() );
        assertTrue( "The Continuous Ranked Probability Skill Score is a skill score.", crpss.isSkillScore() );
        assertTrue( "Expected no decomposition for the Continuous Ranked Probability Skill Score.",
                    crpss.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        assertTrue( "The Continuous Ranked Probability Skill Score is not proper.", !crpss.isProper() );
        assertTrue( "The Continuous Ranked Probability Skill Score is not strictly proper.",
                    !crpss.isStrictlyProper() );
    }

    /**
     * Constructs a {@link ContinuousRankedProbabilitySkillScore} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();

        //Build the metric
        final CRPSSBuilder b = new CRPSSBuilder();
        b.setDecompositionID( ScoreOutputGroup.NONE ).setOutputFactory( outF );
        final ContinuousRankedProbabilitySkillScore crpss = b.build();

        //Check exceptions
        try
        {
            b.setDecompositionID( ScoreOutputGroup.CR_AND_LBR );
            b.build();
            fail( "Expected an exception on building with an unsupported decomposition." );
        }
        catch ( MetricParameterException e )
        {
        }
        //Null input
        try
        {
            crpss.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
        try
        {
            crpss.apply( MetricTestDataFactory.getEnsemblePairsOne() );
            fail( "Expected an exception on null input for the baseline." );
        }
        catch ( MetricInputException e )
        {
        }
        catch ( IOException e )
        {
            fail( "Unable to read the input data." );
        }
    }

}
