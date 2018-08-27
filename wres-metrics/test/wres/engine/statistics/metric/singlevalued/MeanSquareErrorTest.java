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
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.SampleMetadata;
import wres.datamodel.metadata.StatisticMetadata;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link MeanSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanSquareErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link MeanSquareError}.
     */

    private MeanSquareError mse;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.mse = MeanSquareError.of();
    }

    /**
     * Compares the output from {@link MeanSquareError#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        StatisticMetadata m1 = StatisticMetadata.of( SampleMetadata.of( MeasurementUnit.of() ),
                                                     input.getRawData().size(),
                                                     MeasurementUnit.of(),
                                                     MetricConstants.MEAN_SQUARE_ERROR,
                                                     MetricConstants.MAIN );
        //Check the results
        final DoubleScoreStatistic actual = mse.apply( input );
        final DoubleScoreStatistic expected = DoubleScoreStatistic.of( 400003.929, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link MeanSquareError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), SampleMetadata.of() );

        DoubleScoreStatistic actual = mse.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link MeanSquareError#getName()} returns 
     * {@link MetricConstants#MEAN_SQUARE_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( mse.getName().equals( MetricConstants.MEAN_SQUARE_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link MeanSquareError#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( mse.isDecomposable() );
    }

    /**
     * Checks that the {@link MeanSquareError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( mse.isSkillScore() );
    }

    /**
     * Checks that the {@link MeanSquareError#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( mse.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link MeanSquareError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN SQUARE ERROR'." );

        mse.apply( null );
    }

    /**
     * Tests for an expected exception on calling {@link MeanSquareError#aggregate(DoubleScoreStatistic)} with 
     * null input.
     */

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN SQUARE ERROR'." );

        mse.aggregate( null );
    }

}
