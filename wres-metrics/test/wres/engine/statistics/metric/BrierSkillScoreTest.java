package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.engine.statistics.metric.BrierSkillScore.BrierSkillScoreBuilder;
import wres.engine.statistics.metric.inputs.DiscreteProbabilityPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.VectorOutput;

/**
 * Tests the {@link BrierSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class BrierSkillScoreTest
{

    /**
     * Constructs a {@link BrierSkillScore} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1BrierSkillScore()
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        //Build the metric
        final BrierSkillScoreBuilder<DiscreteProbabilityPairs, VectorOutput> b =
                                                                               new BrierSkillScore.BrierSkillScoreBuilder<>();
        b.setDecompositionID(MetricConstants.NONE);

        final BrierSkillScore<DiscreteProbabilityPairs, VectorOutput> bss = b.build();

        //Check the results
        final VectorOutput actual = bss.apply(input);
        final VectorOutput expected = MetricOutputFactory.ofVectorOutput(new double[]{0.11363636363636376}, 6, null);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Brier Skill Score.", bss.getName().equals("Brier Skill Score"));
        assertTrue("The Brier Skill Score is decomposable.", bss.isDecomposable());
        assertTrue("The Brier Skill Score is a skill score.", bss.isSkillScore());
        assertTrue("Expected no decomposition for the Brier Skill Score.",
                   bss.getDecompositionID() == MetricConstants.NONE);
        assertTrue("The Brier Skill Score is not proper.", !bss.isProper());
        assertTrue("The Brier Skill Score is not strictly proper.", !bss.isStrictlyProper());
    }

}
