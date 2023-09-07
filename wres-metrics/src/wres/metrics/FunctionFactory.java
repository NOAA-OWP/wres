package wres.metrics;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ToDoubleFunction;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import wres.datamodel.MissingValues;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;

/**
 * A factory class for constructing elementary functions.
 *
 * @author James Brown
 */

public class FunctionFactory
{
    /** Map of summary statistics. */
    private static final Map<MetricConstants, ToDoubleFunction<double[]>> STATISTICS =
            new EnumMap<>( MetricConstants.class );

    /** Median. */
    private static final Median MEDIAN = new Median();

    /** Mean. */
    private static final Mean MEAN = new Mean();

    /** Standard deviation. */
    private static final StandardDeviation STANDARD_DEVIATION = new StandardDeviation();

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
     * <p>Return a function that computes a skill score from two elementary scores whose perfect score is zero:
     *
     * <p> <code>(a,b) -&gt; 1.0 - (a / b)</code>
     *
     * @return a function that computes the skill for scores whose perfect value is 0
     */

    public static DoubleBinaryOperator skill()
    {
        return ( a, b ) -> finiteOrMissing().applyAsDouble( 1.0 - ( a / b ) );
    }

    /**
     * Return a function that produces the identity of the finite input or {@link MissingValues#DOUBLE} if the
     * input is non-finite.
     *
     * @return a function that computes the finite identity
     */

    public static DoubleUnaryOperator finiteOrMissing()
    {
        return a -> Double.isFinite( a ) ? a : MissingValues.DOUBLE;
    }

    /**
     * Return a function that computes the equality of two doubles to 8 d.p.
     *
     * @return a function that computes the skill
     */

    public static BiPredicate<Double, Double> doubleEquals()
    {
        return ( a, b ) -> Double.isFinite( a ) && Double.isFinite( b ) ? Math.abs( a - b ) < .00000001
                : Double.compare( a, b ) == 0;
    }

    /**
     * Return a function that computes the mean average of a vector of doubles.
     *
     * @return a function that computes the mean over the input
     */

    public static ToDoubleFunction<double[]> mean()
    {
        return MEAN::evaluate;
    }

    /**
     * Return a function that computes the mean average of a vector of doubles.
     *
     * @return a function that computes the mean over the input
     */

    public static ToDoubleFunction<double[]> median()
    {
        return MEDIAN::evaluate;
    }

    /**
     * Return a function that computes the mean average of the absolute values in a vector of doubles.
     *
     * @return a function that computes the mean absolute value over the input
     */

    public static ToDoubleFunction<double[]> meanAbsolute()
    {
        return a -> Arrays.stream( a )
                          .map( Math::abs )
                          .sorted() // Sort for accuracy/consistency: #72568
                          .average()
                          .orElse( MissingValues.DOUBLE );
    }

    /**
     * Return a function that computes the minimum of value in a vector of doubles.
     *
     * @return a function that computes the minimum over the input
     */

    public static ToDoubleFunction<double[]> minimum()
    {
        return a -> Arrays.stream( a )
                          .min()
                          .orElse( MissingValues.DOUBLE );
    }

    /**
     * Return a function that computes the maximum of value in a vector of doubles.
     *
     * @return a function that computes the maximum over the input
     */

    public static ToDoubleFunction<double[]> maximum()
    {
        return a -> Arrays.stream( a )
                          .max()
                          .orElse( MissingValues.DOUBLE );
    }

    /**
     * Return a function that computes the sample standard deviation of a vector of doubles.
     *
     * @return a function that computes the standard deviation over the input
     */

    public static ToDoubleFunction<double[]> standardDeviation()
    {
        return STANDARD_DEVIATION::evaluate;
    }

    /**
     * Return a function that computes the sample standard deviation of a vector of doubles.
     *
     * @param mean the precomputed mean
     * @return a function that computes the standard deviation over the input
     */

    public static ToDoubleFunction<double[]> standardDeviation( double mean )
    {
        return a -> STANDARD_DEVIATION.evaluate( a, mean );
    }

    /**
     * Return a function that computes the maximum of value in a vector of doubles.
     *
     * @return a function that computes the maximum over the input
     */

    public static ToDoubleFunction<double[]> sampleSize()
    {
        return a -> a.length;
    }

    /**
     * Returns a statistic associated with a {@link MetricConstants} that belongs to the 
     * {@link MetricGroup#UNIVARIATE_STATISTIC}.
     *
     * @param statistic the identifier for the statistic
     * @return the statistic
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the input does not belong to {@link MetricGroup#UNIVARIATE_STATISTIC}
     *            or the statistic does not exist
     */

    public static ToDoubleFunction<double[]> ofStatistic( MetricConstants statistic )
    {
        Objects.requireNonNull( statistic );
        if ( !statistic.isInGroup( MetricGroup.UNIVARIATE_STATISTIC ) )
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
