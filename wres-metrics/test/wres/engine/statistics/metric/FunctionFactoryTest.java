package wres.engine.statistics.metric;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Objects;
import java.util.function.BiPredicate;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MissingValues;

/**
 * Tests the {@link FunctionFactory}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class FunctionFactoryTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Data factory.
     */

    private DataFactory dataFac = DefaultDataFactory.getInstance();

    /**
     * Tests for double equality.
     */

    private BiPredicate<Double, Double> doubleTester = FunctionFactory.doubleEquals();

    /**
     * Tests the {@link FunctionFactory#error()}.
     */

    @Test
    public void testError()
    {
        assertTrue( doubleTester.test( FunctionFactory.error().applyAsDouble( dataFac.pairOf( -1, 1 ) ), 2.0 ) );
    }

    @Test
    public void testAbsError()
    {
        assertTrue( doubleTester.test( FunctionFactory.absError().applyAsDouble( dataFac.pairOf( -1, 1 ) ), 2.0 ) );
    }

    @Test
    public void testSquareError()
    {
        assertTrue( doubleTester.test( FunctionFactory.squareError().applyAsDouble( dataFac.pairOf( -5, 5 ) ),
                                       100.0 ) );
    }

    @Test
    public void testSkill()
    {
        assertTrue( doubleTester.test( FunctionFactory.skill().applyAsDouble( 1.0, 2.0 ), 0.5 ) );
    }

    @Test
    public void testRound()
    {
        assertTrue( doubleTester.test( FunctionFactory.round().apply( 2.04, 1 ), 2.0 ) );
    }

    @Test
    public void testDoubleEquals()
    {
        assertTrue( doubleTester.test( 1.0000131, 1.0000131 ) );

        assertFalse( doubleTester.test( 13.13131, 13.13132 ) );

        assertTrue( doubleTester.test( Double.NaN, Double.NaN ) );

        assertTrue( doubleTester.test( Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY ) );

        assertTrue( doubleTester.test( Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY ) );

        assertFalse( doubleTester.test( Double.NaN, Double.NEGATIVE_INFINITY ) );

        assertFalse( doubleTester.test( 1.0, Double.NaN ) );

        assertFalse( doubleTester.test( Double.POSITIVE_INFINITY, 1.0 ) );

    }

    @Test
    public void testFiniteOrMissing()
    {
        assertTrue( doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( 1.13 ), 1.13 ) );

        assertTrue( doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.NEGATIVE_INFINITY ),
                                       MissingValues.MISSING_DOUBLE ) );

        assertTrue( doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.POSITIVE_INFINITY ),
                                       MissingValues.MISSING_DOUBLE ) );

        assertTrue( doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.NaN ),
                                       MissingValues.MISSING_DOUBLE ) );
    }

    @Test
    public void testMean()
    {
        assertTrue( doubleTester.test( FunctionFactory.mean().applyAsDouble( dataFac.vectorOf( new double[] { 1.0, 2.0,
                                                                                                              3.0 } ) ),
                                       2.0 ) );
    }

    @Test
    public void testMedian()
    {
        assertTrue( doubleTester.test( FunctionFactory.median()
                                                      .applyAsDouble( dataFac.vectorOf( new double[] { 4.0, 7.0,
                                                                                                       6.3,
                                                                                                       5.1723 } ) ),
                                       5.73615 ) );
    }

    @Test
    public void testMeanAbsolute()
    {
        assertTrue( doubleTester.test( FunctionFactory.meanAbsolute()
                                                      .applyAsDouble( dataFac.vectorOf( new double[] { 4.3, -2.9, 7,
                                                                                                       13.13131 } ) ),
                                       6.8328275 ) );
    }

    @Test
    public void testMinimum()
    {
        assertTrue( doubleTester.test( FunctionFactory.minimum()
                                                      .applyAsDouble( dataFac.vectorOf( new double[] { 4.3, -2.9, 7,
                                                                                                       13.13131 } ) ),
                                       -2.9 ) );

        assertTrue( doubleTester.test( FunctionFactory.minimum()
                                                      .applyAsDouble( dataFac.vectorOf( new double[] { 4.3,
                                                                                                       Double.NEGATIVE_INFINITY } ) ),
                                       Double.NEGATIVE_INFINITY ) );
    }

    @Test
    public void testMaximum()
    {
        assertTrue( doubleTester.test( FunctionFactory.maximum()
                                                      .applyAsDouble( dataFac.vectorOf( new double[] { 4.3, -2.9, 7,
                                                                                                       13.13131 } ) ),
                                       13.13131 ) );

        assertTrue( doubleTester.test( FunctionFactory.maximum()
                                                      .applyAsDouble( dataFac.vectorOf( new double[] { Double.POSITIVE_INFINITY,
                                                                                                       4.3,
                                                                                                       Double.NEGATIVE_INFINITY } ) ),
                                       Double.POSITIVE_INFINITY ) );
    }

    @Test
    public void testStandardDeviation()
    {
        assertTrue( doubleTester.test( FunctionFactory.standardDeviation()
                                                      .applyAsDouble( dataFac.vectorOf( new double[] { 7, 9, 11, 13,
                                                                                                       123.883 } ) ),
                                       50.97908922 ) );
    }
    
    @Test
    public void testSampleSize()
    {
        assertTrue( doubleTester.test( FunctionFactory.sampleSize()
                                                      .applyAsDouble( dataFac.vectorOf( new double[] { 7, 9, 11, 13,
                                                                                                       123.883 } ) ),
                                       5.0 ) );
    }

    @Test
    public void testOfStatistic()
    {
        assertTrue( Objects.nonNull( FunctionFactory.ofStatistic( MetricConstants.MEAN ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofStatistic( MetricConstants.SAMPLE_SIZE ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofStatistic( MetricConstants.MINIMUM ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofStatistic( MetricConstants.MAXIMUM ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofStatistic( MetricConstants.MEDIAN ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofStatistic( MetricConstants.MEAN_ABSOLUTE ) ) );
        assertTrue( Objects.nonNull( FunctionFactory.ofStatistic( MetricConstants.STANDARD_DEVIATION ) ) );
    }
    
    @Test
    public void testOfStatisticWithWrongInput()
    {
        exception.expect( IllegalArgumentException.class );
        exception.expectMessage( "The statistic 'MAIN' is not a recognized statistic in this context." );
        
        FunctionFactory.ofStatistic( MetricConstants.MAIN );
    }
    
    @Test
    public void testGetSupportUnivariateStatistics()
    {
        assertTrue( Objects.nonNull( FunctionFactory.getSupportedUnivariateStatistics() ) );
    }
    
}
