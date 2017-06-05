package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.engine.statistics.metric.MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.VectorOutput;

/**
 * <p>
 * Tests the {@link MeanSquareErrorSkillScore}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanSquareErrorSkillScoreTest
{

    /**
     * Constructs a {@link MeanSquareErrorSkillScore} and compares the actual result to the expected result. Also,
     * checks the parameters of the metric.
     */

    @Test
    public void test1MeanSquareErrorSkillScore()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Build the metric
        final MeanSquareErrorSkillScoreBuilder<SingleValuedPairs, VectorOutput> b =
                                                                                  new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>();
        final MeanSquareErrorSkillScore<SingleValuedPairs, VectorOutput> mse = b.build();

        //Check the results
        final VectorOutput actual = mse.apply(input);
        final VectorOutput expected = MetricOutputFactory.ofVectorOutput(new double[]{0.8007025335093799}, 10, null);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Mean Square Error Skill Score.",
                   mse.getName().equals("Mean Square Error Skill Score"));
        assertTrue("The Mean Square Error is decomposable.", mse.isDecomposable());
        assertTrue("The Mean Square Error is a skill score.", mse.isSkillScore());
        assertTrue("Expected no decomposition for the Mean Square Error Skill Score.",
                   mse.getDecompositionID() == MetricConstants.NONE);

        //Check the exceptions
        try
        {
            b.setDecompositionID(-999).build();
            fail("Expected an invalid decomposition identifier.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            b.setDecompositionID(-999).build().apply(input);
            fail("Expected an invalid decomposition identifier.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            b.setDecompositionID(MetricConstants.NONE).build().apply(MetricTestDataFactory.getSingleValuedPairsOne()); //No baseline
            fail("Expected a missing baseline.");
        }
        catch(final Exception e)
        {
        }

    }

}
