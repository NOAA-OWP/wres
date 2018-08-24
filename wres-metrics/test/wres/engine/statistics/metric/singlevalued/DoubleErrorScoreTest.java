package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.SingleValuedPairs;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

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

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.score = MeanError.of();
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
        final DoubleScoreStatistic actual = score.apply( input );

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
     * Checks that the {@link DoubleErrorScore#getScoreOutputGroup()} returns {@link ScoreGroup#NONE}.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( score.getScoreOutputGroup() == ScoreGroup.NONE );
    }

    /**
     * Tests for an expected exception on building a {@link DoubleErrorScore} with a missing function.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnMissingFunction() throws MetricParameterException
    {     
        class ExceptionCheck extends DoubleErrorScore<SingleValuedPairs> {

            @Override
            public boolean isSkillScore()
            {
                return false;
            }

            @Override
            public MetricConstants getID()
            {
                return MetricConstants.MEAN_ERROR;
            }

            @Override
            public boolean hasRealUnits()
            {
                return false;
            }
        }
        
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Override or specify a non-null error function for the 'MEAN ERROR'." );
        
        new ExceptionCheck().apply( MetricTestDataFactory.getSingleValuedPairsOne() );
    }  
    
    /**
     * Tests for an expected exception on calling {@link DoubleErrorScore#apply(SingleValuedPairs)} with null 
     * input.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testApplyExceptionOnNullInput() throws MetricParameterException
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN ERROR'." );

        score.apply( null );
    }
    
}
