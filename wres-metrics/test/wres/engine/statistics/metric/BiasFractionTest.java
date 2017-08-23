package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.engine.statistics.metric.BiasFraction.BiasFractionBuilder;

/**
 * Tests the {@link BiasFraction}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class BiasFractionTest
{

    /**
     * Constructs a {@link BiasFraction} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     */

    @Test
    public void test1BiasFraction()
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
                                                                  MetricConstants.BIAS_FRACTION,
                                                                  MetricConstants.MAIN);
        //Build the metric
        final BiasFractionBuilder b = new BiasFraction.BiasFractionBuilder();
        b.setOutputFactory(outF);
        final BiasFraction bf = b.build();

        //Check the results
        final ScalarOutput actual = bf.apply(input);
        final ScalarOutput expected = outF.ofScalarOutput(-0.056796298, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Bias Fraction.",
                   bf.getName().equals(metaFac.getMetricName(MetricConstants.BIAS_FRACTION)));
        assertTrue("The Bias Fraction is not decomposable.", !bf.isDecomposable());
        assertTrue("The Bias Fraction is not a skill score.", !bf.isSkillScore());
        assertTrue("The Bias Fraction cannot be decomposed.", bf.getDecompositionID() == MetricDecompositionGroup.NONE);
    }

}
