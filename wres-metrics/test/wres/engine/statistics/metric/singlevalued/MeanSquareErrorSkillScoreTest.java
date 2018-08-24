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

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.metadata.ReferenceTime;
import wres.datamodel.metadata.TimeWindow;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
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
        final StatisticMetadata m1 = StatisticMetadata.of( input.getRawData().size(),
                                                                 MeasurementUnit.of(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                       "SQIN",
                                                                                       "HEFS",
                                                                                       "ESP" ) );

        //Check the results
        final DoubleScoreStatistic actual = msess.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.8007025335093799, m1 );
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
        final TimeWindow timeWindow = window;
        StatisticMetadata m1 = StatisticMetadata.of( input.getRawData().size(),
                                                           MeasurementUnit.of(),
                                                           MeasurementUnit.of( "MM/DAY" ),
                                                           MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                           MetricConstants.MAIN,
                                                           DatasetIdentifier.of( Location.of( "103.1" ),
                                                                                 "QME",
                                                                                 "NVE" ),
                                                           timeWindow,
                                                           null,
                                                           null  );

        //Check the results
        DoubleScoreStatistic actual = msess.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.7832791707526114, m1 );
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
        StatisticMetadata m1 = StatisticMetadata.of( input.getRawData().size(),
                                                           MeasurementUnit.of(),
                                                           MeasurementUnit.of(),
                                                           MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                                           MetricConstants.MAIN );

        //Check the results
        DoubleScoreStatistic actual = msess.apply( input );
        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 0.9963647159052861, m1 );
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
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );

        DoubleScoreStatistic actual = msess.apply( input );

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
        assertTrue( msess.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link MeanSquareErrorSkillScore#apply(SingleValuedPairs)} with null
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN SQUARE ERROR SKILL SCORE'." );

        msess.apply( null );
    }

}
