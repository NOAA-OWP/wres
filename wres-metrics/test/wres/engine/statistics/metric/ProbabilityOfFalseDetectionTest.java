package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;
import wres.engine.statistics.metric.ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder;

/**
 * Tests the {@link ProbabilityOfFalseDetection}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ProbabilityOfFalseDetectionTest
{

    /**
     * Constructs a {@link ProbabilityOfFalseDetection} and compares the actual result to the expected result. Also,
     * checks the parameters of the metric.
     */

    @Test
    public void test1ProbabilityOfDetection()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(input.getData().size(),
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.PROBABILITY_OF_FALSE_DETECTION,
                                                                    MetricConstants.MAIN,
                                                                    "Main",
                                                                    null);

        //Build the metric
        final ProbabilityOfFalseDetectionBuilder<DichotomousPairs, ScalarOutput> b =
                                                                                   new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder<>();
        final ProbabilityOfFalseDetection<DichotomousPairs, ScalarOutput> pofd = b.build();

        //Check the results
        final ScalarOutput actual = pofd.apply(input);
        final ScalarOutput expected = MetricOutputFactory.ofScalarOutput(0.14615384615384616, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Probability of False Detection.",
                   pofd.getName().equals(MetadataFactory.getMetricName(MetricConstants.PROBABILITY_OF_FALSE_DETECTION)));
        assertTrue("The Probability of False Detection is not decomposable.", !pofd.isDecomposable());
        assertTrue("The Probability of False Detection is not a skill score.", !pofd.isSkillScore());
        assertTrue("The Probability of False Detection cannot be decomposed.",
                   pofd.getDecompositionID() == MetricConstants.NONE);
        final String expName = MetricFactory.ofContingencyTable().getName();
        final String actName = MetadataFactory.getMetricName(pofd.getCollectionOf());           
        assertTrue("The Probability of False Detection should be a collection of '" + expName
            + "', but is actually a collection of '" + actName + "'.",
                   pofd.getCollectionOf()==MetricFactory.ofContingencyTable().getID());
    }

}
