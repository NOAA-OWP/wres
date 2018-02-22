package wres.engine.statistics.metric;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ToDoubleFunction;

import org.apache.commons.math3.stat.descriptive.rank.Median;

import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;

/**
 * A factory class for constructing elementary functions.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class FunctionFactory
{

    /**
     * Map of summary statistics.
     */

    private static final Map<MetricConstants, ToDoubleFunction<VectorOfDoubles>> STATISTICS =
            new EnumMap<>( MetricConstants.class );

    /**
     * Median.
     */

    private static final Median median = new Median();

    /**
     * Return a function that computes the difference between the second and first entries in a {@link PairOfDoubles}.
     * 
     * @return a function that computes the error
     */

    public static DoubleErrorFunction error()
    {
        return a -> a.getItemTwo() - a.getItemOne();
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
        return ( a, b ) -> finiteOrNaN().applyAsDouble ( 1.0 - ( a / b ) );
    }
    
    /**
     * <p>
     * Return a function that produces the identity of the finite input or {@link Double#NaN} if the input is 
     * non-finite.
     * </p>
     * 
     * @return a function that computes the finite identity
     */

    public static DoubleUnaryOperator finiteOrNaN()
    {
        return a -> Double.isFinite( a ) ? a : Double.NaN;
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
     * @return a function that computes the mean over the input
     */

    public static ToDoubleFunction<VectorOfDoubles> mean()
    {
        return a -> Arrays.stream( a.getDoubles() ).average().getAsDouble();
    }

    /**
     * <p>
     * Return a function that computes the mean average of a vector of doubles.
     * </p>
     * 
     * @return a function that computes the mean over the input
     */

    public static ToDoubleFunction<VectorOfDoubles> median()
    {
        return a -> median.evaluate( a.getDoubles() );
    }

    /**
     * <p>
     * Return a function that computes the mean average of the absolute values in a vector of doubles.
     * </p>
     * 
     * @return a function that computes the mean absolute value over the input
     */

    public static ToDoubleFunction<VectorOfDoubles> meanAbsolute()
    {
        return a -> Arrays.stream( a.getDoubles() ).map( Math::abs ).average().getAsDouble();
    }

    /**
     * <p>
     * Return a function that computes the minimum of value in a vector of doubles.
     * </p>
     * 
     * @return a function that computes the minimum over the input
     */

    public static ToDoubleFunction<VectorOfDoubles> minimum()
    {
        return a -> Arrays.stream( a.getDoubles() ).min().getAsDouble();
    }

    /**
     * <p>
     * Return a function that computes the maximum of value in a vector of doubles.
     * </p>
     * 
     * @return a function that computes the maximum over the input
     */

    public static ToDoubleFunction<VectorOfDoubles> maximum()
    {
        return a -> Arrays.stream( a.getDoubles() ).max().getAsDouble();
    }

    /**
     * <p>
     * Return a function that computes the sample standard deviation of a vector of doubles.
     * </p>
     * 
     * @return a function that computes the standard deviation over the input
     */

    public static ToDoubleFunction<VectorOfDoubles> standardDeviation()
    {
        return a -> {
            double mean = mean().applyAsDouble( a );
            return Math.sqrt( Arrays.stream( a.getDoubles() )
                                    .map( d -> Math.pow( d - mean, 2 ) )
                                    .sum()
                              / ( a.size() - 1.0 ) );
        };
    }

    /**
     * Returns a statistic associated with a {@link MetricConstants} that belongs to the 
     * {@link ScoreOutputGroup#UNIVARIATE_STATISTIC}.
     * 
     * @param statistic the identifier for the statistic
     * @return the statistic
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input does not belong to {@link ScoreOutputGroup#UNIVARIATE_STATISTIC} 
     *            or the statistic does not exist
     */

    public static ToDoubleFunction<VectorOfDoubles> ofStatistic( MetricConstants statistic )
    {
        Objects.requireNonNull( statistic );
        if ( !statistic.isInGroup( ScoreOutputGroup.UNIVARIATE_STATISTIC ) )
        {
            throw new IllegalArgumentException( "The statistic '" + statistic
                                                + "' is not a recognized statistic "
                                                + "in this context." );
        }
        // Lazy build the map
        buildStatisticsMap();
        if ( !STATISTICS.containsKey( statistic ) )
        {
            throw new IllegalArgumentException( "The statistic '" + statistic + "' has not been implemented." );
        }
        return STATISTICS.get( statistic );
    }

    /**
     * Returns the set of supported univariate statistics.
     * 
     * @return the univariate statistics
     */

    public static Set<MetricConstants> getSupportedUnivariateStatistics()
    {
        return Collections.unmodifiableSet( STATISTICS.keySet() );
    }

    /**
     * No argument constructor.
     */

    private FunctionFactory()
    {
    };

    /**
     * Builds the map of statistics.
     */

    private static void buildStatisticsMap()
    {
        STATISTICS.put( MetricConstants.MEAN, mean() );
        STATISTICS.put( MetricConstants.MEDIAN, median() );
        STATISTICS.put( MetricConstants.STANDARD_DEVIATION, standardDeviation() );
        STATISTICS.put( MetricConstants.MINIMUM, minimum() );
        STATISTICS.put( MetricConstants.MAXIMUM, maximum() );
        STATISTICS.put( MetricConstants.MEAN_ABSOLUTE, meanAbsolute() );
    }

}
