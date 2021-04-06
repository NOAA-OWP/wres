package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricGroup;
import wres.datamodel.pools.SampleData;
import wres.datamodel.pools.SampleDataException;
import wres.datamodel.pools.pairs.PoolOfPairs;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.engine.statistics.metric.FunctionFactory;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link DoubleErrorScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class DoubleErrorScoreTest
{

    /**
     * Default instance of a {@link MeanError}.
     */

    private DoubleErrorScore<SampleData<Pair<Double, Double>>> score;

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
        final DoubleScoreStatisticOuter actual = score.apply( input );

        //Check the parameters
        assertTrue( "Unexpected baseline identifier for the DoubleErrorScore.",
                    actual.getMetadata()
                          .getEvaluation()
                          .getBaselineDataName()
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
        class ExceptionCheck extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
        {
            private ExceptionCheck()
            {
                super( null, MeanError.METRIC );
            }

            @Override
            public boolean isSkillScore()
            {
                return false;
            }

            @Override
            public MetricConstants getMetricName()
            {
                return MetricConstants.MEAN_ERROR;
            }

            @Override
            public boolean hasRealUnits()
            {
                return false;
            }
        }

        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> new ExceptionCheck().apply( MetricTestDataFactory.getSingleValuedPairsOne() ) );

        assertEquals( "Cannot construct the error score 'MEAN ERROR' with a null error function.",
                      actual.getMessage() );
    }

    @Test
    public void testExceptionOnNullAccumulationFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
        {
            private ExceptionCheck()
            {
                super( FunctionFactory.error(), null, MeanError.METRIC );
            }

            @Override
            public boolean isSkillScore()
            {
                return false;
            }

            @Override
            public MetricConstants getMetricName()
            {
                return MetricConstants.MEAN_ERROR;
            }

            @Override
            public boolean hasRealUnits()
            {
                return false;
            }
        }

        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> new ExceptionCheck().apply( MetricTestDataFactory.getSingleValuedPairsOne() ) );

        assertEquals( "Cannot construct the error score 'MEAN ERROR' with a null "
                + "accumulator function.",
                      actual.getMessage() );
    }

    @Test
    public void testExceptionOnNullErrorFunctionAndNonNullAccumulatorFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<SampleData<Pair<Double, Double>>>
        {
            private ExceptionCheck()
            {
                super( null, FunctionFactory.mean(), MeanError.METRIC );
            }

            @Override
            public boolean isSkillScore()
            {
                return false;
            }

            @Override
            public MetricConstants getMetricName()
            {
                return MetricConstants.MEAN_ERROR;
            }

            @Override
            public boolean hasRealUnits()
            {
                return false;
            }
        }

        NullPointerException actual = assertThrows( NullPointerException.class,
                                                    () -> new ExceptionCheck().apply( MetricTestDataFactory.getSingleValuedPairsOne() ) );

        assertEquals( "Cannot construct the error score 'MEAN ERROR' with a null error function.",
                      actual.getMessage() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        SampleDataException actual = assertThrows( SampleDataException.class,
                                                   () -> this.score.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.score.getName() + "'.", actual.getMessage() );
    }

}
