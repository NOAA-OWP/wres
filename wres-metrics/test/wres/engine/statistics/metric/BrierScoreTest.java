package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricInput;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.VectorOutput;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;
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
     */

    @Test
    public void test1BrierScore()
    {
        //Generate some data
        final MetricInput<?> input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        //Build the metric
        final BrierScoreBuilder b = new BrierScore.BrierScoreBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory(outF);
        b.setDecompositionID(MetricDecompositionGroup.NONE);

        final BrierScore bs = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                                      metaFac.getOutputMetadata(input.size(),
                                                                metaFac.getDimension(),
                                                                metaFac.getDimension(),
                                                                MetricConstants.BRIER_SCORE,
                                                                MetricConstants.MAIN,
                                                                metaFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));

        //Check the results       
        final VectorOutput actual = bs.apply((DiscreteProbabilityPairs)input);
        final VectorOutput expected = outF.ofVectorOutput(new double[]{0.26}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Brier Score.",
                   bs.getName().equals(metaFac.getMetricName(MetricConstants.BRIER_SCORE)));
        assertTrue("The Brier Score is decomposable.", bs.isDecomposable());
        assertTrue("The Brier Score is not a skill score.", !bs.isSkillScore());
        assertTrue("Expected no decomposition for the Brier Score.",
                   bs.getDecompositionID() == MetricDecompositionGroup.NONE);
        assertTrue("The Brier Score is proper.", bs.isProper());
        assertTrue("The Brier Score is strictly proper.", bs.isStrictlyProper());

    }

}
