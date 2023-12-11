package wres.metrics;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.random.EmpiricalDistribution;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.commons.math3.stat.descriptive.rank.Median;

import wres.datamodel.MissingValues;
import wres.config.MetricConstants;
import wres.config.MetricConstants.MetricGroup;
import wres.datamodel.Slicer;
import wres.datamodel.pools.Pool;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.MetricName;
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

    /** Bin axis for a histogram. */
    private static final DiagramMetric.DiagramMetricComponent HISTOGRAM_BINS =
            DiagramMetric.DiagramMetricComponent.newBuilder()
                                                .setName( DiagramMetric.DiagramMetricComponent.DiagramComponentName.BIN_UPPER_BOUND )
                                                .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                                .setMinimum( Double.NEGATIVE_INFINITY )
                                                .setMaximum( Double.POSITIVE_INFINITY )
                                                .build();

    /** Count axis for a histogram. */
    private static final DiagramMetric.DiagramMetricComponent HISTOGRAM_COUNT =
            DiagramMetric.DiagramMetricComponent.newBuilder()
                                                .setName( DiagramMetric.DiagramMetricComponent.DiagramComponentName.COUNT )
                                                .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                .setMinimum( 0 )
                                                .setMaximum( Double.POSITIVE_INFINITY )
                                                .setUnits( "COUNT" )
                                                .build();

    /** Histogram metric. */
    private static final DiagramMetric HISTOGRAM_METRIC = DiagramMetric.newBuilder()
                                                                       .addComponents( FunctionFactory.HISTOGRAM_BINS )
                                                                       .addComponents( FunctionFactory.HISTOGRAM_COUNT )
                                                                       .setName( MetricName.HISTOGRAM )
                                                                       .build();

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
     * @throws IllegalArgumentException if the probability is outside the unit interval
     */

    public static ToDoubleFunction<double[]> quantile( double probability )
    {
        // Valid probability?
        if ( probability < 0.0 || probability > 1.0 )
        {
            throw new IllegalArgumentException( "The supplied probability is invalid : " + probability );
        }

        return samples ->
        {
            // Sort?
            if ( !FunctionFactory.isSorted().test( samples ) )
            {
                Arrays.sort( samples );
            }

            DoubleUnaryOperator quantileFunction = Slicer.getQuantileFunction( samples );

            return quantileFunction.applyAsDouble( probability );
        };
    }

    /**
     * Returns a quantile for sampling uncertainty estimation.
     *
     * @param probability the probability
     * @return a quantile function
     * @throws IllegalArgumentException if the probability is outside the unit interval
     */

    public static SummaryStatisticFunction quantileForSamplingUncertainty( double probability )
    {
        ToDoubleFunction<double[]> quantile = FunctionFactory.quantile( probability );
        SummaryStatistic statistic = SummaryStatistic.newBuilder()
                                                     .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                     .setProbability( probability )
                                                     .setDimension( SummaryStatistic.StatisticDimension.RESAMPLED )
                                                     .build();
        return new SummaryStatisticFunction( statistic, quantile );
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
     * Returns a function that calculates a histogram with a prescribed number of bins.
     * @param parameters the histogram parameters
     * @return the histogram function
     * @throws IllegalArgumentException if the number of bins is less than one
     */
    public static DiagramStatisticFunction histogram( SummaryStatistic parameters )
    {
        int bins = parameters.getHistogramBins();
        SummaryStatistic.StatisticDimension dimension = parameters.getDimension();

        BiFunction<Map<DiagramStatisticFunction.DiagramComponentName, String>, double[], DiagramStatistic> f =
                ( p, d ) ->
                {
                    Objects.requireNonNull( p.get( DiagramStatisticFunction.DiagramComponentName.VARIABLE ),
                                            "Cannot create a histogram without a variable name." );
                    Objects.requireNonNull( p.get( DiagramStatisticFunction.DiagramComponentName.VARIABLE_UNIT ),
                                            "Cannot create a histogram without a variable unit." );

                    String variableName = p.get( DiagramStatisticFunction.DiagramComponentName.VARIABLE );
                    String unitName = p.get( DiagramStatisticFunction.DiagramComponentName.VARIABLE_UNIT );

                    DiagramMetric.DiagramMetricComponent domainAxis = FunctionFactory.HISTOGRAM_BINS.toBuilder()
                                                                                                    .setUnits( unitName )
                                                                                                    .build();

                    DiagramMetric metric = FunctionFactory.HISTOGRAM_METRIC.toBuilder()
                                                                           .setComponents( 0, domainAxis )
                                                                           .build();

                    EmpiricalDistribution empirical = new EmpiricalDistribution( bins );
                    empirical.load( d );

                    List<Double> binUpperBounds = Arrays.stream( empirical.getUpperBounds() )
                                                        .boxed()
                                                        .toList();
                    List<Double> binCounts = empirical.getBinStats()
                                                      .stream()
                                                      .mapToDouble( SummaryStatistics::getN )
                                                      .boxed()
                                                      .toList();

                    DiagramStatistic.DiagramStatisticComponent domainStatistic =
                            DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                                      .setMetric( domainAxis )
                                                                      .setName( variableName )
                                                                      .addAllValues( binUpperBounds )
                                                                      .build();

                    DiagramStatistic.DiagramStatisticComponent rangeStatistic =
                            DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                                      .setMetric( FunctionFactory.HISTOGRAM_COUNT )
                                                                      .setName( variableName )
                                                                      .addAllValues( binCounts )
                                                                      .build();

                    return DiagramStatistic.newBuilder()
                                           .addStatistics( domainStatistic )
                                           .addStatistics( rangeStatistic )
                                           .setMetric( metric )
                                           .build();
                };

        SummaryStatistic summaryStatistic = SummaryStatistic.newBuilder()
                                                            .setStatistic( SummaryStatistic.StatisticName.HISTOGRAM )
                                                            .setDimension( dimension )
                                                            .build();

        return new DiagramStatisticFunction( summaryStatistic, f );
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
     * @throws IllegalArgumentException if the statistic is not a valid summary statistic in this context
     */

    public static SummaryStatisticFunction ofSummaryStatistic( SummaryStatistic statistic )
    {
        Objects.requireNonNull( statistic );

        if ( statistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
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
     * Returns a {@link SummaryStatisticFunction} from a {@link SummaryStatistic}.
     *
     * @param statistic the statistic
     * @return the statistic calculator
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the statistic is not a valid summary statistic in this context
     */

    public static DiagramStatisticFunction ofDiagramSummaryStatistic( SummaryStatistic statistic )
    {
        Objects.requireNonNull( statistic );

        if ( statistic.getStatistic() != SummaryStatistic.StatisticName.HISTOGRAM )
        {
            throw new IllegalArgumentException( "Unsupported diagram statistic: " + statistic.getStatistic() + "." );
        }

        return FunctionFactory.histogram( statistic );
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
     * Returns a function that calculates a duration diagram from a corresponding univariate diagram.
     *
     * @param function the function to translate
     * @param units the time units to use
     * @return the duration diagram function
     */
    public static BiFunction<Map<DiagramStatisticFunction.DiagramComponentName, String>, Duration[],
            DiagramStatistic> ofDurationDiagramFromUnivariateFunction( DiagramStatisticFunction function,
                                                                       ChronoUnit units )
    {
        // Create a function that operates on durations
        return ( p, d ) ->
        {
            double[] decimalDurations = Arrays.stream( d )
                                              .mapToDouble( a -> ( a.getSeconds()
                                                                   * 1000 )
                                                                 + ( a.getNano()
                                                                     / 1_000_000.0 ) )
                                              .map( fu -> fu / units.getDuration()
                                                                    .toMillis() )
                                              .toArray();

            // Replace the units for the domain axis
            DiagramStatistic result = function.apply( p, decimalDurations );
            DiagramStatistic.Builder builder = result.toBuilder();
            String unitString = units.toString()
                                     .toUpperCase();
            builder.getMetricBuilder()
                   .getComponentsBuilder( 0 )
                   .setUnits( unitString );
            builder.getStatisticsBuilder( 0 )
                   .getMetricBuilder()
                   .setUnits( unitString );

            return builder.build();
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
