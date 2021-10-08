package wres.metrics.singlevalued;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.MetricGroup;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.metrics.FunctionFactory;
import wres.metrics.MetricTestDataFactory;

/**
 * Tests the {@link DoubleErrorScore}.
 * 
 * @author James Brown
 */
public final class DoubleErrorScoreTest
{

    /**
     * Default instance of a {@link MeanError}.
     */

    private DoubleErrorScore<Pool<Pair<Double, Double>>> score;

    @Before
    public void setupBeforeEachTest()
    {
        this.score = MeanError.of();
    }

    @Test
    public void testBaseline()
    {

        //Generate some data with a baseline
        final Pool<Pair<Double, Double>> input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Check the results
        final DoubleScoreStatisticOuter actual = this.score.apply( input );

        //Check the parameters
        assertEquals( "ESP",
                      actual.getMetadata()
                            .getEvaluation()
                            .getBaselineDataName() );
    }

    @Test
    public void testIsDecomposable()
    {
        assertFalse( this.score.isDecomposable() );
    }

    @Test
    public void testGetScoreOutputGroup()
    {
        assertSame( MetricGroup.NONE, this.score.getScoreOutputGroup() );
    }

    @Test
    public void testExceptionOnNullErrorFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<Pool<Pair<Double, Double>>>
        {
            private ExceptionCheck()
            {
                super( null, MeanError.METRIC_INNER );
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
                                                    () -> new ExceptionCheck() );

        assertEquals( "Cannot construct the error score 'MEAN ERROR' with a null error function.",
                      actual.getMessage() );
    }

    @Test
    public void testExceptionOnNullAccumulationFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<Pool<Pair<Double, Double>>>
        {
            private ExceptionCheck()
            {
                super( FunctionFactory.error(), null, MeanError.METRIC_INNER );
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
                                                    () -> new ExceptionCheck() );

        assertEquals( "Cannot construct the error score 'MEAN ERROR' with a null "
                      + "accumulator function.",
                      actual.getMessage() );
    }

    @Test
    public void testExceptionOnNullErrorFunctionAndNonNullAccumulatorFunction()
    {
        class ExceptionCheck extends DoubleErrorScore<Pool<Pair<Double, Double>>>
        {
            private ExceptionCheck()
            {
                super( null, FunctionFactory.mean(), MeanError.METRIC_INNER );
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
                                                    () -> new ExceptionCheck() );

        assertEquals( "Cannot construct the error score 'MEAN ERROR' with a null error function.",
                      actual.getMessage() );
    }

    @Test
    public void testExceptionOnNullInput()
    {
        PoolException actual = assertThrows( PoolException.class,
                                             () -> this.score.apply( null ) );

        assertEquals( "Specify non-null input to the '" + this.score.getName() + "'.", actual.getMessage() );
    }

}
