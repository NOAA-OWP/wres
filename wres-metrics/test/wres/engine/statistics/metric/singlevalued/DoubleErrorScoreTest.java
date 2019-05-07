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
import wres.engine.statistics.metric.FunctionFactory;
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
    public void setupBeforeEachTest()
    {
        this.score = MeanError.of();
    }

    /**
     * Checks that the baseline is set correctly.
     */

    @Test
    public void testBaseline()
    {

        //Generate some data with a baseline
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        final DoubleScoreStatistic actual = score.apply( input );

        //Check the parameters
        assertTrue( "Unexpected baseline identifier for the DoubleErrorScore.",
                    actual.getMetadata()
                          .getSampleMetadata()
                          .getIdentifier()
                          .getScenarioIDForBaseline()
                          .equals( "ESP" ) );
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
     * Tests for an expected exception on building a {@link DoubleErrorScore} with a null 
     * error function.
     */

    @Test
    public void testExceptionOnNullErrorFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<SingleValuedPairs>
        {
            private ExceptionCheck()
            {
                super( null );
            }

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

        exception.expect( NullPointerException.class );
        exception.expectMessage( "Cannot construct the error score 'MEAN ERROR' with a null error function." );

        new ExceptionCheck().apply( MetricTestDataFactory.getSingleValuedPairsOne() );
    }

    /**
     * Tests for an expected exception on building a {@link DoubleErrorScore} with a null 
     * accumulation function.
     */

    @Test
    public void testExceptionOnNullAccumulationFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<SingleValuedPairs>
        {
            private ExceptionCheck()
            {
                super( FunctionFactory.error(), null );
            }

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

        exception.expect( NullPointerException.class );
        exception.expectMessage( "Cannot construct the error score 'MEAN ERROR' with a null "
                                 + "accumulator function." );

        new ExceptionCheck().apply( MetricTestDataFactory.getSingleValuedPairsOne() );
    }
    
    /**
     * Tests for an expected exception on building a {@link DoubleErrorScore} with a null 
     * error function and a non-null accumulator function.
     */

    @Test
    public void testExceptionOnNullErrorFunctionAndNonNullAccumulatorFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<SingleValuedPairs>
        {
            private ExceptionCheck()
            {
                super( null, FunctionFactory.mean() );
            }

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

        exception.expect( NullPointerException.class );
        exception.expectMessage( "Cannot construct the error score 'MEAN ERROR' with a null error function." );

        new ExceptionCheck().apply( MetricTestDataFactory.getSingleValuedPairsOne() );
    }    

    /**
     * Tests for an expected exception on calling {@link DoubleErrorScore#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN ERROR'." );

        score.apply( null );
    }

}
