package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.engine.statistics.metric.RootMeanSquareError.RootMeanSquareErrorBuilder;

/**
 * Tests the {@link RootMeanSquareError}.
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

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(input.getData().size(),
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                                                                    MetricConstants.MAIN,
                                                                    null,
                                                                    null);

        //Build the metric
        final RootMeanSquareErrorBuilder b = new RootMeanSquareError.RootMeanSquareErrorBuilder();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.of();
        b.setOutputFactory(outF);
        final RootMeanSquareError mse = b.build();

        //Check the results
        final ScalarOutput actual = mse.apply(input);
        final ScalarOutput expected = outF.ofScalarOutput(632.4586381732801, m1);
        assertTrue("Actual: " + actual.getData() + ". Expected: " + expected.getData() + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Root Mean Square Error.",
                   mse.getName().equals(MetadataFactory.getMetricName(MetricConstants.ROOT_MEAN_SQUARE_ERROR)));
        assertTrue("The Root Mean Square Error is not decomposable.", !mse.isDecomposable());
        assertTrue("The Root Mean Square Error is not a skill score.", !mse.isSkillScore());
        assertTrue("Expected no decomposition for the Root Mean Square Error.",
                   mse.getDecompositionID() == MetricConstants.NONE);

    }

}
