package wres.engine.statistics.metric.ensemble;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

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
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilityScore.CRPSBuilder;

/**
 * Tests the {@link ContinuousRankedProbabilityScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ContinousRankedProbabilityScoreTest
{

    /**
     * Constructs a {@link ContinuousRankedProbabilityScore} and compares the actual result to the expected result for
     * a scenario without missing data. Also, checks the parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1CRPSNoMissings() throws MetricParameterException
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 25.7, new double[] { 23, 43, 45, 23, 54 } ) );
        pairs.add( outF.pairOf( 21.4, new double[] { 19, 16, 57, 23, 9 } ) );
        pairs.add( outF.pairOf( 32.1, new double[] { 23, 54, 23, 12, 32 } ) );
        pairs.add( outF.pairOf( 47, new double[] { 12, 54, 23, 54, 78 } ) );
        pairs.add( outF.pairOf( 12.1, new double[] { 9, 8, 5, 6, 12 } ) );
        pairs.add( outF.pairOf( 43, new double[] { 23, 12, 12, 34, 10 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, metaFac.getMetadata() );

        //Build the metric
        final CRPSBuilder b = new CRPSBuilder();

        b.setDecompositionID( ScoreOutputGroup.NONE ).setOutputFactory( outF );

        final ContinuousRankedProbabilityScore crps = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.NONE );
        //Check the results       
        final DoubleScoreOutput actual = crps.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 7.63, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
        //Check the parameters
        assertTrue( "Unexpected name for the Continuous Ranked Probability Score.",
                    crps.getName()
                        .equals( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE.toString() ) );
        assertTrue( "The Continuous Ranked Probability Score is decomposable.", crps.isDecomposable() );
        assertTrue( "The Continuous Ranked Probability Score is not a skill score.", !crps.isSkillScore() );
        assertTrue( "Expected no decomposition for the Continuous Ranked Probability Score.",
                    crps.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        assertTrue( "The Continuous Ranked Probability Score is proper.", crps.isProper() );
        assertTrue( "The Continuous Ranked Probability Score is strictly proper.", crps.isStrictlyProper() );
    }

    /**
     * Constructs a {@link ContinuousRankedProbabilityScore} and compares the actual result to the expected result for
     * a scenario without  missing data.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2CRPSWithMissings() throws MetricParameterException
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 25.7, new double[] { 23, 43, 45, 34.2, 23, 54 } ) );
        pairs.add( outF.pairOf( 21.4, new double[] { 19, 16, 57, 23, 9 } ) );
        pairs.add( outF.pairOf( 32.1, new double[] { 23, 54, 23, 12, 32, 45.3, 67.1 } ) );
        pairs.add( outF.pairOf( 47, new double[] { 12, 54, 23, 54 } ) );
        pairs.add( outF.pairOf( 12, new double[] { 9, 8, 5 } ) );
        pairs.add( outF.pairOf( 43, new double[] { 23, 12, 12 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, metaFac.getMetadata() );

        //Build the metric
        final CRPSBuilder b = new CRPSBuilder();

        b.setDecompositionID( ScoreOutputGroup.NONE ).setOutputFactory( outF );

        final ContinuousRankedProbabilityScore crps = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.NONE );
        //Check the results       
        final DoubleScoreOutput actual = crps.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 8.734401927437641, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }
    
    /**
     * Constructs a {@link ContinuousRankedProbabilityScore} and compares the actual result to the expected result for
     * a scenario where the observed value overlaps one ensemble member. This exposes a mistake in the Hersbach (2000) 
     * paper where rows 1 and 3 of table/eqn. 26 should be inclusive bounds.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test3CRPSObsEqualsMember() throws MetricParameterException
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 32, new double[] { 23, 54, 23, 12, 32 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, metaFac.getMetadata() );

        //Build the metric
        final CRPSBuilder b = new CRPSBuilder();

        b.setDecompositionID( ScoreOutputGroup.NONE ).setOutputFactory( outF );

        final ContinuousRankedProbabilityScore crps = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.NONE );
        //Check the results       
        final DoubleScoreOutput actual = crps.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 4.56, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }    
    
    /**
     * Constructs a {@link ContinuousRankedProbabilityScore} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test4Exceptions() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();

        //Build the metric
        final CRPSBuilder b = new CRPSBuilder();
        b.setDecompositionID( ScoreOutputGroup.NONE ).setOutputFactory( outF );
        final ContinuousRankedProbabilityScore crps = b.build();

        //Check exceptions
        try
        {
            crps.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch(MetricInputException e)
        {          
        }
    }    

}
