package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.PairedInput;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.VectorOutput;
import wres.engine.statistics.metric.BrierScore.BrierScoreBuilder;

/**
 * Tests the {@link BrierScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class BrierScoreTest
{

    /**
     * Constructs a {@link BrierScore} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1BrierScore() throws MetricParameterException
    {
        //Generate some data
        final PairedInput<?> input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        //Build the metric
        final BrierScoreBuilder b = new BrierScore.BrierScoreBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory( outF );
        b.setDecompositionID( ScoreOutputGroup.NONE );

        final BrierScore bs = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.BRIER_SCORE,
                                           MetricConstants.NONE,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        //Check the results       
        final VectorOutput actual = bs.apply( (DiscreteProbabilityPairs) input );
        final VectorOutput expected = outF.ofVectorOutput( new double[] { 0.26 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );
        //Check the parameters
        assertTrue( "Unexpected name for the Brier Score.",
                    bs.getName().equals( MetricConstants.BRIER_SCORE.toString() ) );
        assertTrue( "The Brier Score is decomposable.", bs.isDecomposable() );
        assertTrue( "The Brier Score is not a skill score.", !bs.isSkillScore() );
        assertTrue( "Expected no decomposition for the Brier Score.",
                    bs.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        assertTrue( "The Brier Score is proper.", bs.isProper() );
        assertTrue( "The Brier Score is strictly proper.", bs.isStrictlyProper() );

    }

}
