package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.VectorOutput;
import wres.engine.statistics.metric.BrierSkillScore.BrierSkillScoreBuilder;

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
        final DiscreteProbabilityPairs input = MetricTestDataFactory2.getDiscreteProbabilityPairsTwo();

        //Build the metric
        final BrierSkillScoreBuilder b = new BrierSkillScore.BrierSkillScoreBuilder();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory(outF);
        b.setDecompositionID(MetricConstants.NONE);

        final BrierSkillScore bss = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.BRIER_SKILL_SCORE,
                                                                  MetricConstants.MAIN,
                                                                  "DRRC2",
                                                                  "SQIN",
                                                                  "HEFS",
                                                                  "ESP");

        //Check the results 
        final VectorOutput actual = bss.apply(input);
        final VectorOutput expected = outF.ofVectorOutput(new double[]{0.11363636363636376}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Brier Skill Score.",
                   bss.getName().equals(metaFac.getMetricName(MetricConstants.BRIER_SKILL_SCORE)));
        assertTrue("The Brier Skill Score is decomposable.", bss.isDecomposable());
        assertTrue("The Brier Skill Score is a skill score.", bss.isSkillScore());
        assertTrue("Expected no decomposition for the Brier Skill Score.",
                   bss.getDecompositionID() == MetricConstants.NONE);
        assertTrue("The Brier Skill Score is not proper.", !bss.isProper());
        assertTrue("The Brier Skill Score is not strictly proper.", !bss.isStrictlyProper());
    }

}
