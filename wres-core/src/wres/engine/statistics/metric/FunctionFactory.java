package wres.engine.statistics.metric;

import java.util.function.DoubleBinaryOperator;
import wres.datamodel.PairOfDoubles;

/**
 * A factory class for constructing elementary functions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class FunctionFactory
{
    /**
     * Return a function that computes the difference between the first and second entries in a {@link PairOfDoubles}.
     * 
     * @return a function that computes the error
     */

    public static DoubleErrorFunction error()
    {
        return (a) -> a.getItemOne() - a.getItemTwo();
    }

    /**
     * Return a function that computes the absolute difference between the first and second entries in a
     * {@link PairOfDoubles}.
     * 
     * @return a function that computes the absolute error
     */

    public static DoubleErrorFunction absError()
    {
        return (a) -> Math.abs(a.getItemOne() - a.getItemTwo());
    }

    /**
     * Return a function that computes the square difference between the first and second entries in a
     * {@link PairOfDoubles}.
     * 
     * @return a function that computes the square error
     */

    public static DoubleErrorFunction squareError()
    {
        return (a) -> Math.pow(a.getItemOne() - a.getItemTwo(), 2);
    }

    /**
     * <p>
     * Return a function that computes a skill score from two elementary scores:
     * </p>
     * <p>
     * <code>(a,b) -&gt; 1.0 - (a / b)</code>
     * </p>
     * 
     * @return a function that computes the skill
     */

    public static DoubleBinaryOperator skill()
    {
        return (a, b) -> 1.0 - (a / b);
    }

    /**
     * No argument constructor.
     */

    private FunctionFactory()
    {
    };

}
