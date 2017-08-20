package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.function.BiPredicate;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;

/**
 * Tests the {@link FunctionFactory}.
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
        final DataFactory metIn = DefaultDataFactory.getInstance();
        final BiPredicate<Double, Double> testMe = FunctionFactory.doubleEquals();
        assertTrue("Failure on inequality test.", !testMe.test(-1.0, 0.0));
        assertTrue("Failure on absolute error function.",
                   testMe.test(FunctionFactory.absError().applyAsDouble(metIn.pairOf(-1, 1)), 2.0));
        assertTrue("Failure on error function.",
                   testMe.test(FunctionFactory.error().applyAsDouble(metIn.pairOf(-1, 1)), -2.0));
        assertTrue("Failure on square error function.",
                   testMe.test(FunctionFactory.squareError().applyAsDouble(metIn.pairOf(-5, 5)), 100.0));
        assertTrue("Failure on skill function.", testMe.test(FunctionFactory.skill().applyAsDouble(1.0, 2.0), 0.5));
        assertTrue("Failure on mean function.",
                   testMe.test(FunctionFactory.mean().applyAsDouble(metIn.vectorOf(new double[]{1.0, 2.0, 3.0})), 2.0));
    }

}
