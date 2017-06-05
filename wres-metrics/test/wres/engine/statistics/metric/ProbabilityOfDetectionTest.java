package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.engine.statistics.metric.ProbabilityOfDetection.ProbabilityOfDetectionBuilder;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * <p>
 * Tests the {@link ProbabilityOfDetection}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ProbabilityOfDetectionTest
{

    /**
     * Constructs a {@link ProbabilityOfDetection} and compares the actual result to the expected result. Also, checks
     * the parameters of the metric.
     */

    @Test
    public void test1ProbabilityOfDetection()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Build the metric
        final ProbabilityOfDetectionBuilder<DichotomousPairs, ScalarOutput> b =
                                                                              new ProbabilityOfDetection.ProbabilityOfDetectionBuilder<>();
        final ProbabilityOfDetection<DichotomousPairs, ScalarOutput> pod = b.build();

        //Check the results
        final ScalarOutput actual = pod.apply(input);
        final ScalarOutput expected = MetricOutputFactory.ofScalarOutput(0.780952380952381, 365, null);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Probability of Detection.",
                   pod.getName().equals("Probability of Detection"));
        assertTrue("The Probability of Detection is not decomposable.", !pod.isDecomposable());
        assertTrue("The Probability of Detection is not a skill score.", !pod.isSkillScore());
        assertTrue("The Probability of Detection cannot be decomposed.",
                   pod.getDecompositionID() == MetricConstants.NONE);
        final String name = MetricFactory.ofContingencyTable().getName();
        assertTrue("The Probability of Detection should be a collection of '" + name
            + "', but is actually a collection of '" + pod.getCollectionOf() + "'.",
                   pod.getCollectionOf().equals(name));
    }

}
