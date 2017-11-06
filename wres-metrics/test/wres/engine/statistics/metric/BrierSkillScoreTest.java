package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.VectorOutput;
import wres.engine.statistics.metric.BrierSkillScore.BrierSkillScoreBuilder;

/**
 * Tests the {@link BrierSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class BrierSkillScoreTest
{

    /**
     * Constructs a {@link BrierSkillScore} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1BrierSkillScore() throws MetricParameterException
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsTwo();

        //Build the metric
        final BrierSkillScoreBuilder b = new BrierSkillScore.BrierSkillScoreBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory( outF );
        b.setDecompositionID( ScoreOutputGroup.NONE );

        final BrierSkillScore bss = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension(),
                                                                   MetricConstants.BRIER_SKILL_SCORE,
                                                                   MetricConstants.NONE,
                                                                   metaFac.getDatasetIdentifier( "DRRC2",
                                                                                                 "SQIN",
                                                                                                 "HEFS",
                                                                                                 "ESP" ) );

        //Check the results 
        final VectorOutput actual = bss.apply( input );
        final VectorOutput expected = outF.ofVectorOutput( new double[] { 0.11363636363636376 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );
        //Check the parameters
        assertTrue( "Unexpected name for the Brier Skill Score.",
                    bss.getName().equals( metaFac.getMetricName( MetricConstants.BRIER_SKILL_SCORE ) ) );
        assertTrue( "The Brier Skill Score is decomposable.", bss.isDecomposable() );
        assertTrue( "The Brier Skill Score is a skill score.", bss.isSkillScore() );
        assertTrue( "Expected no decomposition for the Brier Skill Score.",
                    bss.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        assertTrue( "The Brier Skill Score is not proper.", !bss.isProper() );
        assertTrue( "The Brier Skill Score is not strictly proper.", !bss.isStrictlyProper() );
    }

    /**
     * Constructs a {@link BrierSkillScore} with a climatological baseline and compares the actual result to the 
     * expected result.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test2BrierSkillScore() throws MetricParameterException
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();

        //Build the metric
        final BrierSkillScoreBuilder b = new BrierSkillScore.BrierSkillScoreBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory( outF );
        b.setDecompositionID( ScoreOutputGroup.NONE );

        final BrierSkillScore bss = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension(),
                                                                   MetricConstants.BRIER_SKILL_SCORE,
                                                                   MetricConstants.NONE,
                                                                   metaFac.getDatasetIdentifier( "DRRC2",
                                                                                                 "SQIN",
                                                                                                 "HEFS" ) );

        //Check the results 
        final VectorOutput actual = bss.apply( input );
        final VectorOutput expected = outF.ofVectorOutput( new double[] { -0.040000000000000036 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Constructs a {@link BrierSkillScore} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test3Exceptions() throws MetricParameterException
    {
        //Build the metric
        final BrierSkillScoreBuilder b = new BrierSkillScore.BrierSkillScoreBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        b.setDecompositionID( ScoreOutputGroup.NONE );
        final BrierSkillScore bss = b.build();

        //Check exceptions
        try
        {
            bss.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }

        //Check NaN skill
        assertTrue( "Expected NaN for a forecast with only non-occurrences as the baseline.",
                    Double.isNaN( bss.apply( MetricTestDataFactory.getDiscreteProbabilityPairsFour() )
                                     .getData()
                                     .getDoubles()[0] ) );
    }


}
