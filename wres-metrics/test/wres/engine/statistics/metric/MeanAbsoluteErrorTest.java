package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MeanAbsoluteError.MeanAbsoluteErrorBuilder;

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
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test1MeanAbsoluteError() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                  MetricConstants.MAIN);

        //Build the metric
        final MeanAbsoluteErrorBuilder b = new MeanAbsoluteError.MeanAbsoluteErrorBuilder();
        b.setOutputFactory(outF);
        final MeanAbsoluteError mae = b.build();

        //Check the results
        final ScalarOutput actual = mae.apply(input);
        final ScalarOutput expected = outF.ofScalarOutput(201.37, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Mean Absolute Error.",
                   mae.getName().equals(metaFac.getMetricName(MetricConstants.MEAN_ABSOLUTE_ERROR)));
        assertTrue("The Mean Absolute Error is not decomposable.", !mae.isDecomposable());
        assertTrue("The Mean Absolute Error is not a skill score.", !mae.isSkillScore());
        assertTrue("The Mean Absolute Error cannot be decomposed.",
                   mae.getScoreOutputGroup() == ScoreOutputGroup.NONE);
    }

}
