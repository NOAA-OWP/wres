package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.ProbabilityOfDetection.ProbabilityOfDetectionBuilder;
import wres.engine.statistics.metric.inputs.DichotomousPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * Tests the {@link ProbabilityOfDetection}.
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

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(input.getData().size(),
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.PROBABILITY_OF_DETECTION,
                                                                    MetricConstants.MAIN,
                                                                    "Main",
                                                                    null);
        
        //Build the metric
        final ProbabilityOfDetectionBuilder<DichotomousPairs, ScalarOutput> b =
                                                                              new ProbabilityOfDetection.ProbabilityOfDetectionBuilder<>();
        final ProbabilityOfDetection<DichotomousPairs, ScalarOutput> pod = b.build();

        //Check the results
        final ScalarOutput actual = pod.apply(input);
        final ScalarOutput expected = MetricOutputFactory.ofScalarOutput(0.780952380952381,m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Probability of Detection.",
                   pod.getName().equals(MetricConstants.getMetricName(MetricConstants.PROBABILITY_OF_DETECTION)));
        assertTrue("The Probability of Detection is not decomposable.", !pod.isDecomposable());
        assertTrue("The Probability of Detection is not a skill score.", !pod.isSkillScore());
        assertTrue("The Probability of Detection cannot be decomposed.",
                   pod.getDecompositionID() == MetricConstants.NONE);
        final String expName = MetricFactory.ofContingencyTable().getName();
        final String actName = MetricConstants.getMetricName(pod.getCollectionOf());           
        assertTrue("The Probability of Detection should be a collection of '" + expName
            + "', but is actually a collection of '" + actName + "'.",
                   pod.getCollectionOf()==MetricFactory.ofContingencyTable().getID());
    }

}
