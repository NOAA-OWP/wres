package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MeanSquareErrorSkillScoreSkillScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanSquareErrorSkillScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link MeanSquareErrorSkillScore}.
     */

    private MeanSquareErrorSkillScore msess;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.msess = MeanSquareErrorSkillScore.of();
    }

    /**
     * Compares the output from {@link MeanSquareErrorSkillScore#apply(SingleValuedPairs)} against expected output
     * for pairs with an explicit baseline.
     */

    @Test
    public void testApplyWithBaseline()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Metadata for the output
        final MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( input.getRawData().size(),
                                                                           MetadataFactory.getDimension(),
                                                                           MetadataFactory.getDimension( "CMS" ),
                                                                           MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                           MetricConstants.MAIN,
                                                                           MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "DRRC2" ),
                                                                                                                 "SQIN",
                                                                                                                 "HEFS",
                                                                                                                 "ESP" ) );

        //Check the results
        final DoubleScoreOutput actual = msess.apply( input );
        final DoubleScoreOutput expected = DataFactory.ofDoubleScoreOutput( 0.8007025335093799, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link MeanSquareErrorSkillScore#apply(SingleValuedPairs)} against expected output
     * for pairs without an explicit baseline.
     * @throws IOException if the input data could not be read
     */

    @Test
    public void testApplyWithoutBaseline() throws IOException
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsFive();

        //Metadata for the output
        TimeWindow window = TimeWindow.of( Instant.parse( "1985-01-01T00:00:00Z" ),
                                           Instant.parse( "2010-12-31T11:59:59Z" ),
                                           ReferenceTime.VALID_TIME,
                                           Duration.ofHours( 24 ) );
        MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( input.getRawData().size(),
                                                                     MetadataFactory.getDimension(),
                                                                     MetadataFactory.getDimension( "MM/DAY" ),
                                                                     MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                     MetricConstants.MAIN,
                                                                     MetadataFactory.getDatasetIdentifier( MetadataFactory.getLocation( "103.1" ),
                                                                                                           "QME",
                                                                                                           "NVE" ),
                                                                     window );

        //Check the results
        DoubleScoreOutput actual = msess.apply( input );
        DoubleScoreOutput expected = DataFactory.ofDoubleScoreOutput( 0.7832791707526114, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Compares the output from {@link MeanSquareErrorSkillScore#apply(SingleValuedPairs)} against expected output
     * for pairs without an explicit baseline.
     */

    @Test
    public void testApplyWithoutBaselineTwo()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        MetricOutputMetadata m1 = MetadataFactory.getOutputMetadata( input.getRawData().size(),
                                                                     MetadataFactory.getDimension(),
                                                                     MetadataFactory.getDimension(),
                                                                     MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                     MetricConstants.MAIN );

        //Check the results
        DoubleScoreOutput actual = msess.apply( input );
        DoubleScoreOutput expected = DataFactory.ofDoubleScoreOutput( 0.9963647159052861, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link MeanSquareErrorSkillScore#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                DataFactory.ofSingleValuedPairs( Arrays.asList(), MetadataFactory.getMetadata() );

        DoubleScoreOutput actual = msess.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link MeanSquareErrorSkillScore#getName()} returns
     * {@link MetricConstants#MEAN_SQUARE_ERROR_SKILL_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( msess.getName().equals( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link MeanSquareErrorSkillScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( msess.isDecomposable() );
    }

    /**
     * Checks that the {@link MeanSquareErrorSkillScore#isSkillScore()} returns <code>true</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertTrue( msess.isSkillScore() );
    }

    /**
     * Checks that the {@link MeanSquareErrorSkillScore#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( msess.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link MeanSquareErrorSkillScore#apply(SingleValuedPairs)} with null
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN SQUARE ERROR SKILL SCORE'." );

        msess.apply( null );
    }

}
