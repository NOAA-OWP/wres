package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.function.BiPredicate;

import org.junit.Test;

import wres.datamodel.DataFactory;

/**
 * <p>
 * Tests the {@link FunctionFactory}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class FunctionFactoryTest
{

    /**
     * Tests the methods in {@link FunctionFactory}.
     */

    @Test
    public void test1FunctionFactory()
    {
        final DataFactory d = DataFactory.instance();
        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();
        assertTrue("Failure on inequality test.", !testMe.test(-1.0, 0.0));
        assertTrue("Failure on absolute error function.",
                   testMe.test(FunctionFactory.absError().applyAsDouble(d.pairOf(-1, 1)), 2.0));
        assertTrue("Failure on error function.",
                   testMe.test(FunctionFactory.error().applyAsDouble(d.pairOf(-1, 1)), -2.0));
        assertTrue("Failure on square error function.",
                   testMe.test(FunctionFactory.squareError().applyAsDouble(d.pairOf(-5, 5)), 100.0));
        assertTrue("Failure on skill function.", testMe.test(FunctionFactory.skill().applyAsDouble(1.0, 2.0), 0.5));
    }

}
