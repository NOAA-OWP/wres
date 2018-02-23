package wres.engine.statistics.metric.categorical;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.categorical.PeirceSkillScore.PeirceSkillScoreBuilder;

/**
 * Tests the {@link PeirceSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class PeirceSkillScoreTest
{

    /**
     * Constructs a dichotomous {@link PeirceSkillScore} and compares the actual result to the expected result. Also,
     * checks the parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1PeirceSkillScore() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final DichotomousPairs input = MetricTestDataFactory.getDichotomousPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.PEIRCE_SKILL_SCORE,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );
        //Build the metric
        final PeirceSkillScoreBuilder<DichotomousPairs> b = new PeirceSkillScore.PeirceSkillScoreBuilder<>();
        b.setOutputFactory( outF );
        final PeirceSkillScore<DichotomousPairs> ps = b.build();

        //Check the results
        final DoubleScoreOutput actual = ps.apply( input );
        final MetricFactory metF = MetricFactory.getInstance( outF );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 0.6347985347985348, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
        //Check the parameters
        assertTrue( "Unexpected name for the Peirce Skill Score.",
                    ps.getName().equals( MetricConstants.PEIRCE_SKILL_SCORE.toString() ) );
        assertTrue( "The Peirce Skill Score is not decomposable.", !ps.isDecomposable() );
        assertTrue( "The Peirce Skill Score is a skill score.", ps.isSkillScore() );
        assertTrue( "The Peirce Skill Score cannot be decomposed.",
                    ps.getScoreOutputGroup() == ScoreOutputGroup.NONE );
        final String expName = metF.ofDichotomousContingencyTable().getName();
        final String actName = ps.getCollectionOf().toString();
        assertTrue( "The Peirce Skill Score should be a collection of '" + expName
                    + "', but is actually a collection of '"
                    + actName
                    + "'.",
                    ps.getCollectionOf() == metF.ofDichotomousContingencyTable().getID() );
        //Test exceptions
        try
        {
            ps.aggregate( outF.ofMatrixOutput( new double[][] { { 0.0, 0.0, 0.0 }, { 0.0, 0.0, 0.0 } }, m1 ) );
            fail( "Expected an exception on construction with a a non-square matrix." );
        }
        catch ( final Exception e )
        {
        }
    }

    /**
     * Constructs a multicategory {@link PeirceSkillScore} and compares the actual result to the expected result.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2PeirceSkillScore() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final MulticategoryPairs input = MetricTestDataFactory.getMulticategoryPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.PEIRCE_SKILL_SCORE,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        //Build the metric
        final PeirceSkillScoreBuilder<MulticategoryPairs> b = new PeirceSkillScore.PeirceSkillScoreBuilder<>();
        b.setOutputFactory( outF );
        final PeirceSkillScore<MulticategoryPairs> ps = b.build();

        //Check the results
        final DoubleScoreOutput actual = ps.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 0.05057466520850963, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
        //Test exceptions
        try
        {
            ps.aggregate( outF.ofMatrixOutput( new double[][] { { 0.0, 0.0, 0.0 }, { 0.0, 0.0, 0.0 },
                                                                { 0.0, 0.0, 0.0 } },
                                               m1 ) );
            fail( "Expected a zero sum product." );
        }
        catch ( final Exception e )
        {
        }

    }

}
