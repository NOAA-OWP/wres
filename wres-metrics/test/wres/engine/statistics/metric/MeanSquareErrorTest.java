package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.VectorOutput;
import wres.engine.statistics.metric.MeanSquareError.MeanSquareErrorBuilder;

/**
 * Tests the {@link MeanSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanSquareErrorTest
{

    /**
     * Constructs a {@link MeanSquareError} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1MeanSquareError()
    {
        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getMetadata(input.getData().size(),
                                                                    MetadataFactory.getDimension(),
                                                                    MetricConstants.MEAN_SQUARE_ERROR,
                                                                    MetricConstants.MAIN,
                                                                    null,
                                                                    null);

        //Build the metric
        final MeanSquareErrorBuilder<SingleValuedPairs> b =
                                                                        new MeanSquareError.MeanSquareErrorBuilder<>();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.of();
        b.setOutputFactory(outF);
        final MeanSquareError<SingleValuedPairs> mse = b.build();

        //Check the results
        final VectorOutput actual = mse.apply(input);
        final VectorOutput expected = outF.ofVectorOutput(new double[]{400003.929}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Mean Square Error.",
                   mse.getName().equals(MetadataFactory.getMetricName(MetricConstants.MEAN_SQUARE_ERROR)));
        assertTrue("The Mean Square Error is decomposable.", mse.isDecomposable());
        assertTrue("The Mean Square Error is not a skill score.", !mse.isSkillScore());
        assertTrue("Expected no decomposition for the Mean Square Error.",
                   mse.getDecompositionID() == MetricConstants.NONE);

        //Check the exceptions
        try
        {
            b.setDecompositionID(MetricConstants.BRIER_SCORE).build();
            fail("Expected an invalid decomposition identifier.");
        }
        catch(final Exception e)
        {
        }
        try
        {
            b.setDecompositionID(MetricConstants.BRIER_SCORE).build().apply(input);
            fail("Expected an invalid decomposition identifier.");
        }
        catch(final Exception e)
        {
        }

    }

}
