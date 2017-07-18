package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.DefaultMetricInputFactory;
import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricInputFactory;
import wres.datamodel.metric.MetricOutputFactory;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.engine.statistics.metric.CoefficientOfDetermination.CoefficientOfDeterminationBuilder;

/**
 * Tests the {@link CoefficientOfDetermination}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class CoefficientOfDeterminationTest
{

    /**
     * Constructs a {@link CoefficientOfDetermination}.
     */

    @Test
    public void test1CoefficientOfDetermination()
    {
        //Obtain the factories
        final MetricInputFactory inF = DefaultMetricInputFactory.getInstance();
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Build the metric
        final CoefficientOfDeterminationBuilder b = new CoefficientOfDetermination.CoefficientOfDeterminationBuilder();
        b.setOutputFactory(outF);
        final CoefficientOfDetermination rho = (CoefficientOfDetermination)b.build();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                  MetricConstants.MAIN);

        //Compute normally
        final ScalarOutput actual = rho.apply(input);
        final ScalarOutput expected = outF.ofScalarOutput(Math.pow(0.9999999910148981, 2), m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for Coefficient of determination.",
                   rho.getName().equals(metaFac.getMetricName(MetricConstants.COEFFICIENT_OF_DETERMINATION)));
        assertTrue("Coefficient of determination is not decomposable.", !rho.isDecomposable());
        assertTrue("Coefficient of determination is not a skill score.", !rho.isSkillScore());
        assertTrue("Coefficient of determination cannot be decomposed.",
                   rho.getDecompositionID() == MetricConstants.NONE);
        assertTrue("Coefficient of determination does not have real units", !rho.hasRealUnits());

        //Check exceptions
        List<PairOfDoubles> list = new ArrayList<>();
        list.add(inF.pairOf(0.0, 0.0));
        try
        {
            rho.apply(inF.ofSingleValuedPairs(list, m1));
            fail("Expected a checked exception on invalid inputs: insufficient pairs.");
        }
        catch(MetricCalculationException e)
        {
        }

    }

}
