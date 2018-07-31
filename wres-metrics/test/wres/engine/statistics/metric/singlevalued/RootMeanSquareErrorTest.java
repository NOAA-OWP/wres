package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import wres.datamodel.metadata.Dimension;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link RootMeanSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class RootMeanSquareErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link RootMeanSquareError}.
     */

    private RootMeanSquareError rmse;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.rmse = RootMeanSquareError.of();
    }

    /**
     * Compares the output from {@link RootMeanSquareError#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                                   Dimension.of(),
                                                                   Dimension.of(),
                                                                   MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                                                                   MetricConstants.MAIN );
        //Check the results
        DoubleScoreOutput actual = rmse.apply( input );
        DoubleScoreOutput expected = DoubleScoreOutput.of( 632.4586381732801, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link RootMeanSquareError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );
 
        DoubleScoreOutput actual = rmse.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link RootMeanSquareError#getName()} returns 
     * {@link MetricConstants#ROOT_MEAN_SQUARE_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( rmse.getName().equals( MetricConstants.ROOT_MEAN_SQUARE_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link RootMeanSquareError#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( rmse.isDecomposable() );
    }

    /**
     * Checks that the {@link RootMeanSquareError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( rmse.isSkillScore() );
    }

    /**
     * Checks that the {@link RootMeanSquareError#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( rmse.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }
    
    /**
     * Checks that the {@link RootMeanSquareError#getCollectionOf()} returns 
     * {@link MetricConstants#SUM_OF_SQUARE_ERROR}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( rmse.getCollectionOf().equals( MetricConstants.SUM_OF_SQUARE_ERROR ) );
    }   
    
    /**
     * Checks that the {@link RootMeanSquareError#hasRealUnits()} returns <code>true</code>.
     */

    @Test
    public void testhasRealUnits()
    {
        assertTrue( rmse.hasRealUnits() );
    }

    /**
     * Tests for an expected exception on calling {@link RootMeanSquareError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'ROOT MEAN SQUARE ERROR'." );
        
        rmse.apply( null );
    }    

}
