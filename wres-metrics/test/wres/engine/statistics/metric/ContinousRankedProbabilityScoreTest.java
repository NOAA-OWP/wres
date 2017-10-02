package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.VectorOutput;
import wres.engine.statistics.metric.ContinuousRankedProbabilityScore.CRPSBuilder;

/**
 * Tests the {@link ContinousRankedProbabilityScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ContinousRankedProbabilityScoreTest
{

    /**
     * Constructs a {@link ContinousRankedProbabilityScore} and compares the actual result to the expected result for
     * a scenario without  missing data. Also, checks the parameters of the metric.
     */

    @Test
    public void test1CRPSNoMissings()
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 25.7, new double[] { 23, 43, 45, 23, 54 } ) );
        pairs.add( outF.pairOf( 21.4, new double[] { 19, 16, 57, 23, 9 } ) );
        pairs.add( outF.pairOf( 32, new double[] { 23, 54, 23, 12, 32 } ) );
        pairs.add( outF.pairOf( 47, new double[] { 12, 54, 23, 54, 78 } ) );
        pairs.add( outF.pairOf( 12, new double[] { 9, 8, 5, 6, 12 } ) );
        pairs.add( outF.pairOf( 43, new double[] { 23, 12, 12, 34, 10 } ) );
        EnsemblePairs input = outF.ofEnsemblePairs( pairs, metaFac.getMetadata() );

        //Build the metric
        final CRPSBuilder b = new CRPSBuilder();

        b.setDecompositionID( ScoreOutputGroup.NONE ).setOutputFactory( outF );

        final ContinuousRankedProbabilityScore crps = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.NONE );
        //Check the results       
        final VectorOutput actual = crps.apply( input );
        final VectorOutput expected = outF.ofVectorOutput( new double[] { 6.596666666666667 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );
        //Check the parameters
        assertTrue( "Unexpected name for the Continuous Ranked Probability Score.",
                    crps.getName()
                        .equals( metaFac.getMetricName( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE ) ) );
        assertTrue( "The Continuous Ranked Probability Score is decomposable.", crps.isDecomposable() );
        assertTrue( "The Continuous Ranked Probability Score is not a skill score.", !crps.isSkillScore() );
        assertTrue( "Expected no decomposition for the Continuous Ranked Probability Score.",
                    crps.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        assertTrue( "The Continuous Ranked Probability Score is proper.", crps.isProper() );
        assertTrue( "The Continuous Ranked Probability Score is strictly proper.", crps.isStrictlyProper() );
    }

    /**
     * Constructs a {@link ContinousRankedProbabilityScore} and compares the actual result to the expected result for
     * a scenario without  missing data.
     */

    @Test
    public void test2CRPSWithMissings()
    {
        //Generate some data
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        final List<PairOfDoubleAndVectorOfDoubles> pairs = new ArrayList<>();
        pairs.add( outF.pairOf( 25.7, new double[] { 23, 43, 45, 34.2, 23, 54 } ) );
        pairs.add( outF.pairOf( 21.4, new double[] { 19, 16, 57, 23, 9 } ) );
        pairs.add( outF.pairOf( 32, new double[] { 23, 54, 23, 12, 32, 45.3, 67.1 } ) );
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
                metaFac.getOutputMetadata( input.size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                           MetricConstants.NONE );
        //Check the results       
        final VectorOutput actual = crps.apply( input );
        final VectorOutput expected = outF.ofVectorOutput( new double[] { 8.0493679138322 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );
    }

}
