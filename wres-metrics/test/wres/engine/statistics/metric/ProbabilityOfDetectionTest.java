package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;
import wres.engine.statistics.metric.ProbabilityOfDetection.ProbabilityOfDetectionBuilder;

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
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                                      metaFac.getOutputMetadata(input.getData().size(),
                                                                metaFac.getDimension(),
                                                                metaFac.getDimension(),
                                                                MetricConstants.PROBABILITY_OF_DETECTION,
                                                                MetricConstants.MAIN,
                                                                metaFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"));

        //Build the metric
        final ProbabilityOfDetectionBuilder b = new ProbabilityOfDetection.ProbabilityOfDetectionBuilder();
        b.setOutputFactory(outF);
        final ProbabilityOfDetection pod = b.build();

        //Check the results
        final ScalarOutput actual = pod.apply(input);
        final MetricFactory metF = MetricFactory.getInstance(outF);
        final ScalarOutput expected = outF.ofScalarOutput(0.780952380952381, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Probability of Detection.",
                   pod.getName().equals(metaFac.getMetricName(MetricConstants.PROBABILITY_OF_DETECTION)));
        assertTrue("The Probability of Detection is not decomposable.", !pod.isDecomposable());
        assertTrue("The Probability of Detection is not a skill score.", !pod.isSkillScore());
        assertTrue("The Probability of Detection cannot be decomposed.",
                   pod.getDecompositionID() == MetricDecompositionGroup.NONE);
        final String expName = metF.ofContingencyTable().getName();
        final String actName = metaFac.getMetricName(pod.getCollectionOf());
        assertTrue("The Probability of Detection should be a collection of '" + expName
            + "', but is actually a collection of '" + actName + "'.",
                   pod.getCollectionOf() == metF.ofContingencyTable().getID());
    }

}
