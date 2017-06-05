package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.engine.statistics.metric.RootMeanSquareError.RootMeanSquareErrorBuilder;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * <p>
 * Tests the {@link RootMeanSquareError}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class RootMeanSquareErrorTest
{

    /**
     * Constructs a {@link RootMeanSquareError} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1RootMeanSquareError()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Build the metric
        final RootMeanSquareErrorBuilder<SingleValuedPairs, ScalarOutput> b =
                                                                            new RootMeanSquareError.RootMeanSquareErrorBuilder<>();
        final RootMeanSquareError<SingleValuedPairs, ScalarOutput> mse = b.build();

        //Check the results
        final ScalarOutput actual = mse.apply(input);
        final ScalarOutput expected = MetricOutputFactory.ofScalarOutput(632.4586381732801, 10, null);
        assertTrue("Actual: " + actual.getData() + ". Expected: " + expected.getData() + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Root Mean Square Error.", mse.getName().equals("Root Mean Square Error"));
        assertTrue("The Root Mean Square Error is not decomposable.", !mse.isDecomposable());
        assertTrue("The Root Mean Square Error is not a skill score.", !mse.isSkillScore());
        assertTrue("Expected no decomposition for the Root Mean Square Error.",
                   mse.getDecompositionID() == MetricConstants.NONE);

    }

}
