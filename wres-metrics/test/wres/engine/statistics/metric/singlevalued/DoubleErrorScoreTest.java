package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.MeanError.MeanErrorBuilder;

/**
 * Tests the {@link DoubleErrorScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DoubleErrorScoreTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link MeanError}.
     */

    private DoubleErrorScore<SingleValuedPairs> score;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        this.score = b.build();
    }
    
    /**
     * Checks that the baseline is set correctly.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void testBaseline() throws MetricParameterException
    {
       
        //Generate some data with a baseline
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        final DoubleScoreOutput actual = score.apply( input );

        //Check the parameters
        assertTrue( "Unexpected baseline identifier for the DoubleErrorScore.",
                    actual.getMetadata().getIdentifier().getScenarioIDForBaseline().equals( "ESP" ) );
    }

    /**
     * Checks that the {@link DoubleErrorScore#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( score.isDecomposable() );
    }
    
    /**
     * Checks that the {@link DoubleErrorScore#getScoreOutputGroup()} returns {@link ScoreOutputGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( score.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on building a {@link DoubleErrorScore} with a missing function.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnMissingFunction() throws MetricParameterException
    {     
        MeanErrorBuilder builder = new MeanErrorBuilder();
        builder.setErrorFunction( null );
        builder.setOutputFactory( DefaultDataFactory.getInstance() );
        MeanError error = builder.build();
        error.f = null;
        
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Override or specify a non-null error function for the 'MEAN ERROR'." );
        
        error.apply( MetricTestDataFactory.getSingleValuedPairsOne() );
    }  
    
    /**
     * Tests for an expected exception on calling {@link DoubleErrorScore#apply(SingleValuedPairs)} with null 
     * input.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testApplyExceptionOnNullInput() throws MetricParameterException
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN ERROR'." );

        score.apply( null );
    }
    
}
