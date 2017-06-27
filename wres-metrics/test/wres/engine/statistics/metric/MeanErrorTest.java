package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.MeanError.MeanErrorBuilder;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * Tests the {@link MeanError}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanErrorTest
{

    /**
     * Constructs a {@link MeanError} and compares the actual result to the expected result. Also, checks the parameters
     * of the metric.
     */

    @Test
    public void test1MeanError()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(input.getData().size(),
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.MEAN_ERROR,
                                                                    MetricConstants.MAIN,
                                                                    null,
                                                                    null);
        //Build the metric
        final MeanErrorBuilder<SingleValuedPairs, ScalarOutput> b = new MeanError.MeanErrorBuilder<>();
        final MeanError<SingleValuedPairs, ScalarOutput> me = b.build();

        //Check the results
        final ScalarOutput actual = me.apply(input);
        final ScalarOutput expected = MetricOutputFactory.ofScalarOutput(-200.55, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Mean Error.",
                   me.getName().equals(MetricConstants.getMetricName(MetricConstants.MEAN_ERROR)));
        assertTrue("The Mean Error is not decomposable.", !me.isDecomposable());
        assertTrue("The Mean Error is not a skill score.", !me.isSkillScore());
        assertTrue("The Mean Error cannot be decomposed.", me.getDecompositionID() == MetricConstants.NONE);
    }

}
