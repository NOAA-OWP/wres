package wres.engine.statistics.metric;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.DoubleBinaryOperator;
import java.util.function.ToDoubleFunction;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfDoubles;

/**
 * A factory class for constructing elementary functions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

class FunctionFactory
{
    /**
     * Return a function that computes the difference between the first and second entries in a {@link PairOfDoubles}.
     * 
     * @return a function that computes the error
     */

    public static DoubleErrorFunction error()
    {
        return a -> a.getItemOne() - a.getItemTwo();
    }

    /**
     * Return a function that computes the absolute difference between the first and second entries in a
     * {@link PairOfDoubles}.
     * 
     * @return a function that computes the absolute error
     */

    public static DoubleErrorFunction absError()
    {
        return a -> Math.abs( a.getItemOne() - a.getItemTwo() );
    }

    /**
     * Return a function that computes the square difference between the first and second entries in a
     * {@link PairOfDoubles}.
     * 
     * @return a function that computes the square error
     */

    public static DoubleErrorFunction squareError()
    {
        return a -> Math.pow( a.getItemOne() - a.getItemTwo(), 2 );
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
        return ( a, b ) -> 1.0 - ( a / b );
    }

    /**
     * Rounds the input to the prescribed number of decimal places using {@link BigDecimal#ROUND_HALF_UP}.
     * 
     * @return a function that rounds to a prescribed number of decimal places
     */

    public static BiFunction<Double, Integer, Double> round()
    {
        return ( input, digits ) -> {
            BigDecimal bd = new BigDecimal( Double.toString( input ) ); //Always use String constructor
            bd = bd.setScale( digits, BigDecimal.ROUND_HALF_UP );
            return bd.doubleValue();
        };
    }

    /**
     * <p>
     * Return a function that computes the equality of two doubles to 8 d.p.
     * </p>
     * 
     * @return a function that computes the skill
     */

    public static BiPredicate<Double, Double> doubleEquals()
    {
        return ( a, b ) -> Math.abs( a - b ) < .00000001;
    }

    /**
     * <p>
     * Return a function that computes the mean average of a vector of doubles.
     * </p>
     * 
     * @return a function that computes the mean
     */

    public static ToDoubleFunction<VectorOfDoubles> mean()
    {
        return a -> Arrays.stream( a.getDoubles() ).average().getAsDouble();
    }

    /**
     * <p>
     * Return a function that computes the sample standard deviation of a vector of doubles.
     * </p>
     * 
     * @return a function that computes the standard deviation
     */

    public static ToDoubleFunction<VectorOfDoubles> standardDeviation()
    {
        return a -> {
            double mean = mean().applyAsDouble( a );
            return Math.sqrt( Arrays.stream( a.getDoubles() )
                                    .map( d -> Math.pow( d - mean, 2 ) )
                                    .sum()
                              / (a.size() - 1.0) );
        };
    }

    /**
     * No argument constructor.
     */

    private FunctionFactory()
    {
    };

}
