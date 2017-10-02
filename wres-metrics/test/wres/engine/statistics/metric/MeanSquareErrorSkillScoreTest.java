package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.VectorOutput;
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
     * Constructs a {@link MeanSquareErrorSkillScore} with an explicit baseline and compares the actual result to the
     * expected result. Also, checks the parameters of the metric.
     */

    @Test
    public void test1MeanSquareErrorSkillScoreWithBaseline()
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("CMS"),
                                                                  MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                  MetricConstants.NONE,
                                                                  metaFac.getDatasetIdentifier("DRRC2",
                                                                                               "SQIN",
                                                                                               "HEFS",
                                                                                               "ESP"));

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
                   mse.getScoreOutputGroup() == ScoreOutputGroup.NONE);

        //Check the exceptions
        try
        {
            b.setDecompositionID(null).build();
            fail("Expected an invalid decomposition identifier.");
        }
        catch(final Exception e)
        {
        }

    }

    /**
     * Constructs a {@link MeanSquareErrorSkillScore} with a no-skill baseline and compares the actual result to
     * the expected result.
     */

    @Test
    public void test2MeanSquareErrorSkillScoreWithoutBaseline()
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        SingleValuedPairs input = null;
        try
        {
            input = MetricTestDataFactory.getSingleValuedPairsFive();
        }
        catch(IOException e)
        {
            fail("Unable to read the test data.");
        }
        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension("MM/DAY"),
                                                                  MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                  MetricConstants.NONE,
                                                                  metaFac.getDatasetIdentifier("103.1", "QME", "NVE"),
                                                                  24);

        //Build the metric
        final MeanSquareErrorSkillScoreBuilder<SingleValuedPairs> b =
                                                                    new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>();
        b.setOutputFactory(outF);
        final MeanSquareErrorSkillScore<SingleValuedPairs> mse = b.build();

        //Check the results
        final VectorOutput actual = mse.apply(input);

        final VectorOutput expected = outF.ofVectorOutput(new double[]{0.7832791707548252}, m1);
        assertTrue("Actual: " + actual.getData().getDoubles()[0] + ". Expected: " + expected.getData().getDoubles()[0]
            + ".", actual.equals(expected));
    }

}
