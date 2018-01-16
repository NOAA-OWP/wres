package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.MultiValuedScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder;

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
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1MeanSquareErrorSkillScoreWithBaseline() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                   MetricConstants.NONE,
                                                                   metaFac.getDatasetIdentifier( "DRRC2",
                                                                                                 "SQIN",
                                                                                                 "HEFS",
                                                                                                 "ESP" ) );

        //Build the metric
        final MeanSquareErrorSkillScoreBuilder<SingleValuedPairs> b =
                new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>();
        b.setOutputFactory( outF );
        final MeanSquareErrorSkillScore<SingleValuedPairs> mse = b.build();

        //Check the results
        final MultiValuedScoreOutput actual = mse.apply( input );
        final MultiValuedScoreOutput expected = outF.ofMultiValuedScoreOutput( new double[] { 0.8007025335093799 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for the Mean Square Error Skill Score.",
                    mse.getName().equals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.toString() ) );
        assertTrue( "The Mean Square Error is decomposable.", mse.isDecomposable() );
        assertTrue( "The Mean Square Error is a skill score.", mse.isSkillScore() );
        assertTrue( "Expected no decomposition for the Mean Square Error Skill Score.",
                    mse.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Constructs a {@link MeanSquareErrorSkillScore} with a no-skill baseline and compares the actual result to
     * the expected result.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2MeanSquareErrorSkillScoreWithoutBaseline() throws MetricParameterException
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
        catch ( IOException e )
        {
            fail( "Unable to read the test data." );
        }
        //Metadata for the output
        final TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                                 Instant.parse( "2010-12-31T11:59:59Z" ),
                                                 ReferenceTime.VALID_TIME,
                                                 Duration.ofHours( 24 ) );
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension( "MM/DAY" ),
                                                                   MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                   MetricConstants.NONE,
                                                                   metaFac.getDatasetIdentifier( "103.1",
                                                                                                 "QME",
                                                                                                 "NVE" ),
                                                                   window );

        //Build the metric
        final MeanSquareErrorSkillScoreBuilder<SingleValuedPairs> b =
                new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>();
        b.setOutputFactory( outF );
        final MeanSquareErrorSkillScore<SingleValuedPairs> mse = b.build();

        //Check the results
        final MultiValuedScoreOutput actual = mse.apply( input );

        final MultiValuedScoreOutput expected = outF.ofMultiValuedScoreOutput( new double[] { 0.7832791707548252 }, m1 );
        assertTrue( "Actual: " + actual.getData().getDoubles()[0]
                    + ". Expected: "
                    + expected.getData().getDoubles()[0]
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Constructs a {@link MeanSquareErrorSkillScore} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test3Exceptions() throws MetricParameterException
    {

        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MeanSquareErrorSkillScoreBuilder<SingleValuedPairs> b =
                new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>();
        b.setOutputFactory( outF );
        final MeanSquareErrorSkillScore<SingleValuedPairs> mse = b.build();

        //Check the exceptions
        try
        {
            ((MeanSquareErrorSkillScoreBuilder<SingleValuedPairs>)b.setDecompositionID( null )).build();
            fail( "Expected an invalid decomposition identifier." );
        }
        catch ( final Exception e )
        {
        }
        try
        {
            mse.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }

}
