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
import wres.engine.statistics.metric.MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder;

/**
 * Tests the {@link MeanSquareErrorSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanSquareErrorSkillScoreTest
{

    /**
     * Constructs a {@link MeanSquareErrorSkillScore} and compares the actual result to the expected result. Also,
     * checks the parameters of the metric.
     */

    @Test
    public void test1MeanSquareErrorSkillScore()
    {
        //Obtain the factories
        final MetricOutputFactory outF = DefaultMetricOutputFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory2.getSingleValuedPairsTwo();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                  MetricConstants.MAIN,
                                                                  "DRRC2",
                                                                  "SQIN",
                                                                  "HEFS",
                                                                  "ESP");

        //Build the metric
        final MeanSquareErrorSkillScoreBuilder<SingleValuedPairs> b =
                                                                    new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>();
        b.setOutputFactory(outF);
        final MeanSquareErrorSkillScore<SingleValuedPairs> mse = b.build();

        //Check the results
        final VectorOutput actual = mse.apply(input);
        final VectorOutput expected = outF.ofVectorOutput(new double[]{0.8007025335093799}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Mean Square Error Skill Score.",
                   mse.getName().equals(metaFac.getMetricName(MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE)));
        assertTrue("The Mean Square Error is decomposable.", mse.isDecomposable());
        assertTrue("The Mean Square Error is a skill score.", mse.isSkillScore());
        assertTrue("Expected no decomposition for the Mean Square Error Skill Score.",
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
        try
        {
            //No baseline
            b.setDecompositionID(MetricConstants.NONE).build().apply(MetricTestDataFactory2.getSingleValuedPairsOne());
            fail("Expected a missing baseline.");
        }
        catch(final Exception e)
        {
        }

    }

}
