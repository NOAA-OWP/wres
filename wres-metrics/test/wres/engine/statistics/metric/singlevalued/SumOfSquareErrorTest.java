package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.metadata.DatasetIdentifier;
import wres.datamodel.metadata.Location;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.sampledata.MetricInputException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
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
    public void setupBeforeEachTest() throws MetricParameterException
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
        final MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MeasurementUnit.of( "CMS" ),
                                                                 MetricConstants.SUM_OF_SQUARE_ERROR,
                                                                 MetricConstants.MAIN,
                                                                 DatasetIdentifier.of( Location.of( "DRRC2" ),
                                                                                       "SQIN",
                                                                                       "HEFS" ) );
        //Check the results
        DoubleScoreOutput actual = sse.apply( input );

        DoubleScoreOutput expected = DoubleScoreOutput.of( 4000039.29, m1 );

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
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );

        DoubleScoreOutput actual = sse.apply( input );

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
        assertTrue( sse.getScoreOutputGroup() == ScoreOutputGroup.NONE );
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
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'SUM OF SQUARE ERROR'." );

        sse.apply( null );
    }

    /**
     * Tests for an expected exception on calling {@link SumOfSquareError#aggregate(DoubleScoreOutput)} with 
     * null input.
     */

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'SUM OF SQUARE ERROR'." );

        sse.aggregate( null );
    }

}
