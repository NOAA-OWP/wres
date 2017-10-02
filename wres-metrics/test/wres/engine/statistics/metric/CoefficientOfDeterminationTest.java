package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
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
        final DataFactory dataF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = dataF.getMetadataFactory();

        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Build the metric
        final CoefficientOfDeterminationBuilder b = new CoefficientOfDetermination.CoefficientOfDeterminationBuilder();
        b.setOutputFactory(dataF);
        final CoefficientOfDetermination cod = b.build();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                  MetricConstants.MAIN);

        //Compute normally
        final ScalarOutput actual = cod.apply(input);
        final ScalarOutput expected = dataF.ofScalarOutput(Math.pow(0.9999999910148981, 2), m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for Coefficient of determination.",
                   cod.getName().equals(metaFac.getMetricName(MetricConstants.COEFFICIENT_OF_DETERMINATION)));
        assertTrue("Coefficient of determination is not decomposable.", !cod.isDecomposable());
        assertTrue("Coefficient of determination is not a skill score.", !cod.isSkillScore());
        assertTrue("Coefficient of determination cannot be decomposed.",
                   cod.getScoreOutputGroup() == ScoreOutputGroup.NONE);
        assertTrue("Coefficient of determination does not have real units", !cod.hasRealUnits());
       
        //Check exceptions
        List<PairOfDoubles> list = new ArrayList<>();
        list.add(dataF.pairOf(0.0, 0.0));
        try
        {
            cod.apply(dataF.ofSingleValuedPairs(list, m1));
            fail("Expected a checked exception on invalid inputs: insufficient pairs.");
        }
        catch(MetricCalculationException e)
        {
        }

    }

}
