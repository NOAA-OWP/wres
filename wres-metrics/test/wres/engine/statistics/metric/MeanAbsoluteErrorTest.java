package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.engine.statistics.metric.MeanAbsoluteError.MeanAbsoluteErrorBuilder;
import wres.engine.statistics.metric.inputs.SingleValuedPairs;
import wres.engine.statistics.metric.outputs.MetricOutputFactory;
import wres.engine.statistics.metric.outputs.ScalarOutput;

/**
 * Tests the {@link MeanAbsoluteError}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanAbsoluteErrorTest
{

    /**
     * Constructs a {@link MeanAbsoluteError} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1MeanAbsoluteError()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(input.getData().size(),
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                    MetricConstants.MAIN,
                                                                    null,
                                                                    null);

        //Build the metric
        final MeanAbsoluteErrorBuilder<SingleValuedPairs, ScalarOutput> b =
                                                                          new MeanAbsoluteError.MeanAbsoluteErrorBuilder<>();
        final MeanAbsoluteError<SingleValuedPairs, ScalarOutput> mae = b.build();

        //Check the results
        final ScalarOutput actual = mae.apply(input);
        final ScalarOutput expected = MetricOutputFactory.ofScalarOutput(201.37, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Mean Absolute Error.",
                   mae.getName().equals(MetricConstants.getMetricName(MetricConstants.MEAN_ABSOLUTE_ERROR)));
        assertTrue("The Mean Absolute Error is not decomposable.", !mae.isDecomposable());
        assertTrue("The Mean Absolute Error is not a skill score.", !mae.isSkillScore());
        assertTrue("The Mean Absolute Error cannot be decomposed.", mae.getDecompositionID() == MetricConstants.NONE);
    }

}
