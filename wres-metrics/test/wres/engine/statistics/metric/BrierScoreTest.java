package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.BrierScore.BrierScoreBuilder;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.VectorOutput;

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
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        //Build the metric
        final BrierScoreBuilder<DiscreteProbabilityPairs, VectorOutput> b = new BrierScore.BrierScoreBuilder<>();
        b.setDecompositionID(MetricConstants.NONE);

        final BrierScore<DiscreteProbabilityPairs, VectorOutput> bs = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(input.getMetadata().getSampleSize(),
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.BRIER_SCORE,
                                                                    MetricConstants.MAIN,
                                                                    "Main",
                                                                    null);

        //Check the results       
        final VectorOutput actual = bs.apply(input);
        final VectorOutput expected = MetricOutputFactory.ofVectorOutput(new double[]{0.26}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Brier Score.",
                   bs.getName().equals(MetricConstants.getMetricName(MetricConstants.BRIER_SCORE)));
        assertTrue("The Brier Score is decomposable.", bs.isDecomposable());
        assertTrue("The Brier Score is not a skill score.", !bs.isSkillScore());
        assertTrue("Expected no decomposition for the Brier Score.", bs.getDecompositionID() == MetricConstants.NONE);
        assertTrue("The Brier Score is proper.", bs.isProper());
        assertTrue("The Brier Score is strictly proper.", bs.isStrictlyProper());

    }

}
