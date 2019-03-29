package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link SumOfSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SumOfSquareErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link SumOfSquareError}.
     */

    private SumOfSquareError sse;

    @Before
    public void setupBeforeEachTest()
    {
        this.sse = SumOfSquareError.of();
    }

    /**
     * Compares the output from {@link SumOfSquareError#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Metadata for the output
        final StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of( "CMS" ),
                                                                              DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                                    "SQIN",
                                                                                                    "HEFS" ) ),
                                                           input.getRawData().size(),
                                                           MeasurementUnit.of( "CMS" ),
                                                           MetricConstants.SUM_OF_SQUARE_ERROR,
                                                           MetricConstants.MAIN );

        //Check the results
        DoubleScoreStatistic actual = sse.apply( input );

        DoubleScoreStatistic expected = DoubleScoreStatistic.of( 4000039.29, m1 );

        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link SumOfSquareError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = sse.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link SumOfSquareError#getName()} returns 
     * {@link MetricConstants#SUM_OF_SQUARE_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( sse.getName().equals( MetricConstants.SUM_OF_SQUARE_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link SumOfSquareError#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( sse.isDecomposable() );
    }

    /**
     * Checks that the {@link SumOfSquareError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( sse.isSkillScore() );
    }

    /**
     * Checks that the {@link SumOfSquareError#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( sse.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Checks that the {@link SumOfSquareError#getCollectionOf()} returns 
     * {@link MetricConstants#SUM_OF_SQUARE_ERROR}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( sse.getCollectionOf().equals( MetricConstants.SUM_OF_SQUARE_ERROR ) );
    }

    /**
     * Tests for an expected exception on calling {@link SumOfSquareError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'SUM OF SQUARE ERROR'." );

        sse.apply( null );
    }

    /**
     * Tests for an expected exception on calling {@link SumOfSquareError#aggregate(DoubleScoreStatistic)} with 
     * null input.
     */

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'SUM OF SQUARE ERROR'." );

        sse.aggregate( null );
    }

}
