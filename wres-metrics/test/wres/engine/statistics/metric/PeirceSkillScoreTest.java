package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MulticategoryPairs;
import wres.datamodel.metric.ScalarOutput;
import wres.engine.statistics.metric.PeirceSkillScore.PeirceSkillScoreBuilder;

/**
 * Tests the {@link PeirceSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class PeirceSkillScoreTest
{

    /**
     * Constructs a dichotomous {@link PeirceSkillScore} and compares the actual result to the expected result. Also,
     * checks the parameters of the metric.
     */

    @Test
    public void test1PeirceSkillScore()
    {
        //Obtain the factories
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getMetadata(input.getData().size(),
                                                            metaFac.getDimension(),
                                                            MetricConstants.PEIRCE_SKILL_SCORE,
                                                            MetricConstants.MAIN,
                                                            "Main",
                                                            null);
        //Build the metric
        final PeirceSkillScoreBuilder<DichotomousPairs> b = new PeirceSkillScore.PeirceSkillScoreBuilder<>();
        b.setOutputFactory(outF);
        final PeirceSkillScore<DichotomousPairs> ps = b.build();

        //Check the results
        final ScalarOutput actual = ps.apply(input);
        final MetricFactory metF = MetricFactory.getInstance(outF);
        final ScalarOutput expected = outF.ofScalarOutput(0.6347985347985348, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Peirce Skill Score.",
                   ps.getName().equals(metaFac.getMetricName(MetricConstants.PEIRCE_SKILL_SCORE)));
        assertTrue("The Peirce Skill Score is not decomposable.", !ps.isDecomposable());
        assertTrue("The Peirce Skill Score is a skill score.", ps.isSkillScore());
        assertTrue("The Peirce Skill Score cannot be decomposed.", ps.getDecompositionID() == MetricConstants.NONE);
        final String expName = metF.ofContingencyTable().getName();
        final String actName = metaFac.getMetricName(ps.getCollectionOf());
        assertTrue("The Peirce Skill Score should be a collection of '" + expName
            + "', but is actually a collection of '" + actName + "'.",
                   ps.getCollectionOf() == metF.ofContingencyTable().getID());
        //Test exceptions
        try
        {
            ps.apply(outF.ofMatrixOutput(new double[][]{{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}}, m1));
            fail("Expected an exception on construction with a a non-square matrix.");
        }
        catch(final Exception e)
        {
        }
    }

    /**
     * Constructs a multicategory {@link PeirceSkillScore} and compares the actual result to the expected result.
     */

    @Test
    public void test2PeirceSkillScore()
    {
        //Obtain the factories
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final MulticategoryPairs input = MetricTestDataFactory.getMulticategoryPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getMetadata(input.getData().size(),
                                                            metaFac.getDimension(),
                                                            MetricConstants.PEIRCE_SKILL_SCORE,
                                                            MetricConstants.MAIN,
                                                            "Main",
                                                            null);

        //Build the metric
        final PeirceSkillScoreBuilder<MulticategoryPairs> b = new PeirceSkillScore.PeirceSkillScoreBuilder<>();
        b.setOutputFactory(outF);
        final PeirceSkillScore<MulticategoryPairs> ps = b.build();

        //Check the results
        final ScalarOutput actual = ps.apply(input);
        final ScalarOutput expected = outF.ofScalarOutput(0.05057466520850963, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Test exceptions
        try
        {
            ps.apply(outF.ofMatrixOutput(new double[][]{{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}}, m1));
            fail("Expected a zero sum product.");
        }
        catch(final Exception e)
        {
        }

    }

}
