package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.engine.statistics.metric.PeirceSkillScore.PeirceSkillScoreBuilder;
import wres.engine.statistics.metric.PeirceSkillScore.PeirceSkillScoreMulticategoryBuilder;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.inputs.MulticategoryPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

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
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Build the metric
        final PeirceSkillScoreBuilder<DichotomousPairs, ScalarOutput> b =
                                                                        new PeirceSkillScore.PeirceSkillScoreBuilder<>();
        final PeirceSkillScore<DichotomousPairs, ScalarOutput> ps = b.build();

        //Check the results
        final ScalarOutput actual = ps.apply(input);
        final ScalarOutput expected = MetricOutputFactory.ofScalarOutput(0.6347985347985348, 365, null);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Peirce Skill Score.", ps.getName().equals("Peirce Skill Score"));
        assertTrue("The Peirce Skill Score is not decomposable.", !ps.isDecomposable());
        assertTrue("The Peirce Skill Score is a skill score.", ps.isSkillScore());
        assertTrue("The Peirce Skill Score cannot be decomposed.", ps.getDecompositionID() == MetricConstants.NONE);
        final String name = MetricFactory.ofContingencyTable().getName();
        assertTrue("The Peirce Skill Score should be a collection of '" + name + "', but is actually a collection of '"
            + ps.getCollectionOf() + "'.", ps.getCollectionOf().equals(name));
        //Test exceptions
        try
        {
            ps.apply(MetricOutputFactory.ofMatrixOutput(new double[][]{{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0}}, 1, null));
            fail("Expected a non-square matrix.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            ps.apply(MetricOutputFactory.ofVectorOutput(new double[]{0.0, 0.0, 0.0}, 1, null));
            fail("Expected a vector input.");
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
        //Generate some data
        final MulticategoryPairs input = MetricTestDataFactory.getMulticategoryPairsOne();

        //Build the metric
        final PeirceSkillScoreMulticategoryBuilder<MulticategoryPairs, ScalarOutput> b =
                                                                                       new PeirceSkillScore.PeirceSkillScoreMulticategoryBuilder<>();
        final PeirceSkillScore<MulticategoryPairs, ScalarOutput> ps = b.build();

        //Check the results
        final ScalarOutput actual = ps.apply(input);
        final ScalarOutput expected = MetricOutputFactory.ofScalarOutput(0.05057466520850963, 788, null);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Test exceptions
        try
        {
            ps.apply(MetricOutputFactory.ofMatrixOutput(new double[][]{{0.0, 0.0, 0.0}, {0.0, 0.0, 0.0},
                {0.0, 0.0, 0.0}}, 1, null));
            fail("Expected a zero sum product.");
        }
        catch(final Exception e)
        {
        }

    }

}
