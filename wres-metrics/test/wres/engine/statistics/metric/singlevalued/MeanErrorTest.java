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
 * Tests the {@link MeanError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanErrorTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link MeanError}.
     */

    private MeanError meanError;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.meanError = MeanError.of();
    }

    /**
     * Compares the output from {@link MeanError#apply(SingleValuedPairs)} against expected output.
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
                                                                   MetricConstants.MEAN_ERROR,
                                                                   MetricConstants.MAIN );
        //Check the results
        final DoubleScoreOutput actual = this.meanError.apply( input );
        final DoubleScoreOutput expected = DoubleScoreOutput.of( 200.55, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link MeanError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );
 
        DoubleScoreOutput actual = meanError.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link MeanError#getName()} returns 
     * {@link MetricConstants#MEAN_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( meanError.getName().equals( MetricConstants.MEAN_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link MeanError#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( meanError.isDecomposable() );
    }

    /**
     * Checks that the {@link MeanError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( meanError.isSkillScore() );
    }

    /**
     * Checks that the {@link MeanError#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( meanError.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link MeanError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN ERROR'." );
        
        meanError.apply( null );
    }    
  
}
