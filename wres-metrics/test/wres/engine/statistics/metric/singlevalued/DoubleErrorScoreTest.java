package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataException;
import wres.datamodel.sampledata.pairs.PoolOfPairs;
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

    private DoubleErrorScore<SampleData<Pair<Double,Double>>> score;

    @Before
    public void setupBeforeEachTest()
    {
        this.score = MeanError.of();
    }

    @Test
    public void testBaseline()
    {

        //Generate some data with a baseline
        final PoolOfPairs<Double, Double> input = MetricTestDataFactory.getSingleValuedPairsTwo();

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

    @Test
    public void testIsDecomposable()
    {
        assertFalse( score.isDecomposable() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( score.getScoreOutputGroup() == MetricGroup.NONE );
    }

    @Test
    public void testExceptionOnNullErrorFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<SampleData<Pair<Double,Double>>>
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

    @Test
    public void testExceptionOnNullAccumulationFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<SampleData<Pair<Double,Double>>>
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
    
    @Test
    public void testExceptionOnNullErrorFunctionAndNonNullAccumulatorFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<SampleData<Pair<Double,Double>>>
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

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( SampleDataException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN ERROR'." );

        score.apply( null );
    }

}
