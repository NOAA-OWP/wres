package wres.metrics;

import java.util.Objects;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import wres.datamodel.MissingValues;
import wres.config.MetricConstants;

/**
 * Tests the {@link FunctionFactory}.
 *
 * @author James Brown
 */
class FunctionFactoryTest
{
    /** Tests for double equality. */
    private final BiPredicate<Double, Double> doubleTester = FunctionFactory.doubleEquals();

    /**
     * Tests the {@link FunctionFactory#error()}.
     */

    @Test
    void testError()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.error().applyAsDouble( Pair.of( -1.0, 1.0 ) ), 2.0 ) );
    }

    @Test
    void testAbsError()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.absError().applyAsDouble( Pair.of( -1.0, 1.0 ) ),
                                            2.0 ) );
    }

    @Test
    void testSquareError()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.squareError().applyAsDouble( Pair.of( -5.0, 5.0 ) ),
                                            100.0 ) );
    }

    @Test
    void testSkill()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.skill().applyAsDouble( 1.0, 2.0 ), 0.5 ) );
    }

    @Test
    void testDoubleEquals()
    {
        assertTrue( this.doubleTester.test( 1.0000131, 1.0000131 ) );

        assertFalse( this.doubleTester.test( 13.13131, 13.13132 ) );

        assertTrue( this.doubleTester.test( Double.NaN, Double.NaN ) );

        assertTrue( this.doubleTester.test( Double.NEGATIVE_INFINITY, Double.NEGATIVE_INFINITY ) );

        assertTrue( this.doubleTester.test( Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY ) );

        assertFalse( this.doubleTester.test( Double.NaN, Double.NEGATIVE_INFINITY ) );

        assertFalse( this.doubleTester.test( 1.0, Double.NaN ) );

        assertFalse( this.doubleTester.test( Double.POSITIVE_INFINITY, 1.0 ) );

    }

    @Test
    void testFiniteOrMissing()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( 1.13 ), 1.13 ) );

        assertTrue( this.doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.NEGATIVE_INFINITY ),
                                            MissingValues.DOUBLE ) );

        assertTrue( this.doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.POSITIVE_INFINITY ),
                                            MissingValues.DOUBLE ) );

        assertTrue( this.doubleTester.test( FunctionFactory.finiteOrMissing().applyAsDouble( Double.NaN ),
                                            MissingValues.DOUBLE ) );
    }

    @Test
    void testMean()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.mean()
                                                           .applyAsDouble( new double[] { 1.0, 2.0, 3.0 } ),
                                            2.0 ) );
    }

    @Test
    void testMedian()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.median()
                                                           .applyAsDouble( new double[] { 4.0, 7.0, 6.3, 5.1723 } ),
                                            5.73615 ) );
    }

    @Test
    void testMeanAbsolute()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.meanAbsolute()
                                                           .applyAsDouble( new double[] { 4.3, -2.9, 7, 13.13131 } ),
                                            6.8328275 ) );
    }

    @Test
    void testMinimum()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.minimum()
                                                           .applyAsDouble( new double[] { 4.3, -2.9, 7, 13.13131 } ),
                                            -2.9 ) );

        assertTrue( this.doubleTester.test( FunctionFactory.minimum()
                                                           .applyAsDouble( new double[] { 4.3,
                                                                   Double.NEGATIVE_INFINITY } ),
                                            Double.NEGATIVE_INFINITY ) );
    }

    @Test
    void testMaximum()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.maximum()
                                                           .applyAsDouble( new double[] { 4.3, -2.9, 7, 13.13131 } ),
                                            13.13131 ) );

        assertTrue( this.doubleTester.test( FunctionFactory.maximum()
                                                           .applyAsDouble( new double[] { Double.POSITIVE_INFINITY,
                                                                   4.3,
                                                                   Double.NEGATIVE_INFINITY } ),
                                            Double.POSITIVE_INFINITY ) );
    }

    @Test
    void testStandardDeviation()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.standardDeviation()
                                                           .applyAsDouble( new double[] { 7, 9, 11, 13, 123.883 } ),
                                            50.97908922 ) );
    }

    @Test
    void testSampleSize()
    {
        assertTrue( this.doubleTester.test( FunctionFactory.sampleSize()
                                                           .applyAsDouble( new double[] { 7, 9, 11, 13, 123.883 } ),
                                            5.0 ) );
    }

    @Test
    void testOfStatistic()
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
    void testOfStatisticWithWrongInput()
    {
        IllegalArgumentException exception = assertThrows( IllegalArgumentException.class,
                                                           () -> FunctionFactory.ofStatistic( MetricConstants.MAIN ) );

        assertEquals( "The statistic 'MAIN' is not a recognized statistic in this context.", exception.getMessage() );
    }
}
