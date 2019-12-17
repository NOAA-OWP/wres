package wres.engine.statistics.metric;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
import wres.datamodel.MetricConstants.ScoreGroup;
import wres.datamodel.MissingValues;
import wres.datamodel.VectorOfDoubles;

/**
 * A factory class for constructing elementary functions.
 * 
 * @author james.brown@hydrosolved.com
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
     * Return a function that computes the difference between the second and first entries in a single-valued pair.
     * 
     * @return a function that computes the error
     */

    public static DoubleErrorFunction error()
    {
        return a -> a.getRight() - a.getLeft();
    }

    /**
     * Return a function that computes the absolute difference between the first and second entries in a single-valued 
     * pair.
     * 
     * @return a function that computes the absolute error
     */

    public static DoubleErrorFunction absError()
    {
        return a -> Math.abs( a.getLeft() - a.getRight() );
    }

    /**
     * Return a function that computes the square difference between the first and second entries in a single-valued 
     * pair.
     * 
     * @return a function that computes the square error
     */

    public static DoubleErrorFunction squareError()
    {
        return a -> Math.pow( a.getLeft() - a.getRight(), 2 );
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
        return ( a, b ) -> finiteOrMissing().applyAsDouble( 1.0 - ( a / b ) );
    }

    /**
     * <p>
     * Return a function that produces the identity of the finite input or {@link MissingValues#DOUBLE} if the 
     * input is non-finite.
     * </p>
     * 
     * @return a function that computes the finite identity
     */

    public static DoubleUnaryOperator finiteOrMissing()
    {
        return a -> Double.isFinite( a ) ? a : MissingValues.DOUBLE;
    }

    /**
     * Rounds the input to the prescribed number of decimal places using {@link BigDecimal#ROUND_HALF_UP}.
     * 
     * @return a function that rounds to a prescribed number of decimal places
     */

    public static BiFunction<Double, Integer, Double> round()
    {
        return ( input, digits ) -> {

            if ( Double.isFinite( input ) )
            {
                BigDecimal bd = new BigDecimal( Double.toString( input ) ); //Always use String constructor
                bd = bd.setScale( digits, RoundingMode.HALF_UP );

                return bd.doubleValue();
            }

            return input;
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
        return ( a, b ) -> Double.isFinite( a ) && Double.isFinite( b ) ? Math.abs( a - b ) < .00000001
                                                                        : Double.compare( a, b ) == 0;
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
        return a -> Arrays.stream( a.getDoubles() )
                          .sorted() // Sort for accuracy/consistency: #72568
                          .average()
                          .getAsDouble();
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
        return a -> Arrays.stream( a.getDoubles() )
                          .map( Math::abs )
                          .sorted() // Sort for accuracy/consistency: #72568
                          .average()
                          .getAsDouble();
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
        return a -> Arrays.stream( a.getDoubles() )
                          .min()
                          .getAsDouble();
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
        return a -> Arrays.stream( a.getDoubles() )
                          .max()
                          .getAsDouble();
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
            double mean = FunctionFactory.mean().applyAsDouble( a );
            return Math.sqrt( Arrays.stream( a.getDoubles() )
                                    .map( d -> Math.pow( d - mean, 2 ) )
                                    .sorted() // Sort for accuracy/consistency: #72568
                                    .sum()
                              / ( a.size() - 1.0 ) );
        };
    }

    /**
     * <p>
     * Return a function that computes the maximum of value in a vector of doubles.
     * </p>
     * 
     * @return a function that computes the maximum over the input
     */

    public static ToDoubleFunction<VectorOfDoubles> sampleSize()
    {
        return VectorOfDoubles::size;
    }

    /**
     * Returns a statistic associated with a {@link MetricConstants} that belongs to the 
     * {@link ScoreGroup#UNIVARIATE_STATISTIC}.
     * 
     * @param statistic the identifier for the statistic
     * @return the statistic
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input does not belong to {@link ScoreGroup#UNIVARIATE_STATISTIC} 
     *            or the statistic does not exist
     */

    public static ToDoubleFunction<VectorOfDoubles> ofStatistic( MetricConstants statistic )
    {
        Objects.requireNonNull( statistic );
        if ( !statistic.isInGroup( ScoreGroup.UNIVARIATE_STATISTIC ) )
        {
            throw new IllegalArgumentException( "The statistic '" + statistic
                                                + "' is not a recognized statistic "
                                                + "in this context." );
        }

        // Lazy build the map
        buildStatisticsMap();

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
    }

    /**
     * Builds the map of statistics.
     */

    private static void buildStatisticsMap()
    {
        STATISTICS.put( MetricConstants.MEAN, FunctionFactory.mean() );
        STATISTICS.put( MetricConstants.MEDIAN, FunctionFactory.median() );
        STATISTICS.put( MetricConstants.STANDARD_DEVIATION, FunctionFactory.standardDeviation() );
        STATISTICS.put( MetricConstants.MINIMUM, FunctionFactory.minimum() );
        STATISTICS.put( MetricConstants.MAXIMUM, FunctionFactory.maximum() );
        STATISTICS.put( MetricConstants.MEAN_ABSOLUTE, FunctionFactory.meanAbsolute() );
        STATISTICS.put( MetricConstants.SAMPLE_SIZE, FunctionFactory.sampleSize() );
    }

}
