package wres.metrics;

import java.time.Duration;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiPredicate;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import wres.datamodel.MissingValues;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.Slicer;
import wres.datamodel.pools.Pool;
import wres.statistics.generated.SummaryStatistic;

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
     * Return a function that computes a quantile for the prescribed probability. The function will only sort the input
     * array if it is not already sorted, accepting a linear/seek penalty always to save a non-linear/sort penalty
     * sometimes.
     *
     * @param probability the probability associated with the quantile
     * @return a function that computes a quantile
     */

    public static ToDoubleFunction<double[]> quantile( double probability )
    {
        return samples ->
        {
            // Sort?
            if( !FunctionFactory.isSorted().test( samples ) )
            {
                Arrays.sort( samples );
            }

            DoubleUnaryOperator quantileFunction = Slicer.getQuantileFunction( samples );

            return quantileFunction.applyAsDouble( probability );
        };
    }

    /**
     * Returns a function that checks for sortedness of a double array. The function has O(N) complexity.
     * @return a function that checks for sortedness
     */
    public static Predicate<double[]> isSorted()
    {
        return check ->
        {
            Objects.requireNonNull( check );

            for ( int i = 0; i < check.length - 1; i++ )
            {
                if ( check[i] > check[i + 1] )
                {
                    return false;
                }
            }

            return true;
        };
    }

    /**
     * Return a function that computes the sum of square errors from a pool, mapping the pool values to real values
     * using the supplied mapper.
     * @param <T> the pool data type
     * @param mapper the mapper
     * @return the function that computes the sum of square errors
     */

    public static <T> ToDoubleFunction<Pool<Pair<T, T>>> sumOfSquareErrors( ToDoubleFunction<T> mapper )
    {
        return pool ->
        {
            List<Pair<T, T>> data = pool.get();
            double sum = 0.0;

            // Data available
            if ( !data.isEmpty() )
            {
                for ( Pair<T, T> next : data )
                {
                    double left = mapper.applyAsDouble( next.getLeft() );
                    double right = mapper.applyAsDouble( next.getRight() );
                    sum += Math.pow( right - left, 2 );
                }
            }

            return sum;
        };
    }

    /**
     * Return a function that computes the sum of square errors from a pool when using the mean left value as the
     * prediction.
     * @param mapper the mapper to convert from pooled values to double values
     * @param <T> the pool data type
     * @return the function that computes the sum of square errors relative to the mean left value
     */

    public static <T> ToDoubleFunction<Pool<Pair<T, T>>> sumOfSquareErrorsForMeanLeft( ToDoubleFunction<T> mapper )
    {
        return pool ->
        {
            List<Pair<T, T>> data = pool.get();
            double sum = 0.0;

            // Data available
            if ( !data.isEmpty() )
            {
                double sumLeft = data.stream()
                                     .mapToDouble( n -> mapper.applyAsDouble( n.getLeft() ) )
                                     .sum();
                double meanLeft = sumLeft / data.size();
                for ( Pair<T, T> next : data )
                {
                    double left = mapper.applyAsDouble( next.getLeft() );
                    sum += Math.pow( meanLeft - left, 2 );
                }
            }

            return sum;
        };
    }

    /**
     * Returns a statistic associated with a {@link MetricConstants} that belongs to the 
     * {@link MetricGroup#UNIVARIATE_STATISTIC}.
     *
     * @param statistic the identifier for the statistic
     * @return the statistic
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the statistic is not a valid summary statistic
     */

    public static ToDoubleFunction<double[]> ofSummaryStatistic( MetricConstants statistic )
    {
        Objects.requireNonNull( statistic );
        if ( !statistic.isInGroup( MetricGroup.UNIVARIATE_STATISTIC ) )
        {
            throw new IllegalArgumentException( "The statistic '" + statistic
                                                + "' is not a recognized statistic "
                                                + "in this context." );
        }

        // Lazy build the map
        FunctionFactory.buildStatisticsMap();

        return STATISTICS.get( statistic );
    }

    /**
     * Returns a {@link SummaryStatisticFunction} from a {@link SummaryStatistic}.
     *
     * @param statistic the statistic
     * @return the statistic calculator
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the statistic is not a valid summary statistic
     */

    public static SummaryStatisticFunction ofSummaryStatistic( SummaryStatistic statistic )
    {
        Objects.requireNonNull( statistic );

        if( statistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
        {
            return new SummaryStatisticFunction( statistic,
                                                 FunctionFactory.quantile( statistic.getProbability() ) );
        }

        MetricConstants name = MetricConstants.valueOf( statistic.getStatistic()
                                                                 .name() );

        ToDoubleFunction<double[]> calculator = FunctionFactory.ofSummaryStatistic( name );

        return new SummaryStatisticFunction( statistic, calculator );
    }

    /**
     * Returns a function that operates on durations from a function that operates on real values. This implementation
     * is lossy insofar as the durations are converted to milliseconds. A non-lossy implementation would require the
     * use of {@link java.math.BigDecimal}, which is expensive and unlikely to be justified for most practical
     * applications.
     *
     * @param univariate the univariate function, required
     * @return the duration function
     * @throws NullPointerException if the univariate function is null
     */
    public static Function<Duration[], Duration> ofDurationFromUnivariateFunction( ToDoubleFunction<double[]> univariate )
    {
        Objects.requireNonNull( univariate );

        return durations ->
        {
            // Convert the input to double ms
            double[] input = Arrays.stream( durations )
                                   .filter( Objects::nonNull )
                                   .mapToDouble( a -> ( a.getSeconds()
                                                        * 1000 )
                                                      + ( a.getNano()
                                                          / 1_000_000.0 ) )
                                   .toArray();

            double measure = univariate.applyAsDouble( input );

            // Round to the nearest ms
            long milliseconds = Math.round( measure );

            return Duration.ofMillis( milliseconds );
        };
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
