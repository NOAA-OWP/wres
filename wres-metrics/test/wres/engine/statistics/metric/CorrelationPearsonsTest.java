package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.engine.statistics.metric.CorrelationPearsons.CorrelationPearsonsBuilder;

/**
 * Tests the {@link CorrelationPearsons}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class CorrelationPearsonsTest
{

    /**
     * Constructs a {@link CorrelationPearsons}. Minimal test that focuses on the wrapper, and not the underlying
     * {@link PearsonsCorrelation}.
     */

    @Test
    public void test1Correlation()
    {
        //Obtain the factories
        final DataFactory dataF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = dataF.getMetadataFactory();

        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Build the metric
        final CorrelationPearsonsBuilder b = new CorrelationPearsons.CorrelationPearsonsBuilder();
        b.setOutputFactory(dataF);
        final CorrelationPearsons rho = b.build();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.CORRELATION_PEARSONS,
                                                                  MetricConstants.MAIN);

        //Compute normally
        final ScalarOutput actual = rho.apply(input);
        final ScalarOutput expected = dataF.ofScalarOutput(0.9999999910148981, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for Pearson's correlation coefficient.",
                   rho.getName().equals(metaFac.getMetricName(MetricConstants.CORRELATION_PEARSONS)));
        assertTrue("Pearson's correlation is not decomposable.", !rho.isDecomposable());
        assertTrue("Pearson's correlation is not a skill score.", !rho.isSkillScore());
        assertTrue("Pearson's correlation cannot be decomposed.",
                   rho.getDecompositionID() == MetricDecompositionGroup.NONE);
        assertTrue("Pearson's correlation does not have real units", !rho.hasRealUnits());

        //Check exceptions
        List<PairOfDoubles> list = new ArrayList<>();
        list.add(dataF.pairOf(0.0, 0.0));
        try
        {
            rho.apply(dataF.ofSingleValuedPairs(list, m1));
            fail("Expected a checked exception on invalid inputs: insufficient pairs.");
        }
        catch(MetricCalculationException e)
        {
        }

    }

}
