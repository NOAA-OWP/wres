package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;
import wres.engine.statistics.metric.CriticalSuccessIndex.CriticalSuccessIndexBuilder;

/**
 * Tests the {@link CriticalSuccessIndex}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class CriticalSuccessIndexTest
{

    /**
     * Constructs a {@link CriticalSuccessIndex} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1CriticalSuccessIndex()
    {
        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(input.getData().size(),
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.CRITICAL_SUCCESS_INDEX,
                                                                    MetricConstants.MAIN,
                                                                    "Main",
                                                                    null);

        //Build the metric
        final CriticalSuccessIndexBuilder b = new CriticalSuccessIndex.CriticalSuccessIndexBuilder();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.of();
        b.setOutputFactory(outF);
        final CriticalSuccessIndex csi = b.build();
        
        //Check the results
        final ScalarOutput actual = csi.apply(input);
        final MetricFactory metF = MetricFactory.of(outF);
        final ScalarOutput expected = outF.ofScalarOutput(0.5734265734265734, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        
        //Check the parameters
        assertTrue("Unexpected name for the Critical Success Index.",
                   csi.getName().equals(MetadataFactory.getMetricName(MetricConstants.CRITICAL_SUCCESS_INDEX)));
        assertTrue("The Critical Success Index is not decomposable.", !csi.isDecomposable());
        assertTrue("The Critical Success Index is not a skill score.", !csi.isSkillScore());
        assertTrue("The Critical Success Index cannot be decomposed.",
                   csi.getDecompositionID() == MetricConstants.NONE);
        final String expName = metF.ofContingencyTable().getName();
        final String actName = MetadataFactory.getMetricName(csi.getCollectionOf());        
        assertTrue("The Critical Success Index should be a collection of '" + expName
            + "', but is actually a collection of '" + actName + "'.",
                   csi.getCollectionOf()==metF.ofContingencyTable().getID());
    }

}
