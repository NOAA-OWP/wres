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
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.units.Units;
import wres.statistics.generated.BoxplotMetric;
import wres.statistics.generated.BoxplotStatistic;
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
    /** Default probabilities. */
    static final List<Double> DEFAULT_PROBABILITIES = List.of( 0.0, 0.25, 0.5, 0.75, 1.0 );

    /** Map of summary statistics. */
    private static final Map<MetricConstants, ToDoubleFunction<double[]>> STATISTICS =
            new EnumMap<>( MetricConstants.class );

    /** Median. Not thread-safe, so avoid stateful operations within this class.*/
    private static final Median MEDIAN = new Median();

    /** Mean. Not thread-safe, so avoid stateful operations within this class.*/
    private static final Mean MEAN = new Mean();

    /** Standard deviation. Not thread-safe, so avoid stateful operations within this class. */
    private static final StandardDeviation STANDARD_DEVIATION = new StandardDeviation();

    /** Bin axis for a histogram. */
    private static final DiagramMetric.DiagramMetricComponent HISTOGRAM_BINS =
            DiagramMetric.DiagramMetricComponent.newBuilder()
                                                .setName( MetricName.BIN_UPPER_BOUND )
                                                .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_DOMAIN_AXIS )
                                                .setMinimum( Double.NEGATIVE_INFINITY )
                                                .setMaximum( Double.POSITIVE_INFINITY )
                                                .build();

    /** Count axis for a histogram. */
    private static final DiagramMetric.DiagramMetricComponent HISTOGRAM_COUNT =
            DiagramMetric.DiagramMetricComponent.newBuilder()
                                                .setName( MetricName.COUNT )
                                                .setType( DiagramMetric.DiagramMetricComponent.DiagramComponentType.PRIMARY_RANGE_AXIS )
                                                .setMinimum( 0 )
                                                .setMaximum( Double.POSITIVE_INFINITY )
                                                .setUnits( Units.COUNT )
                                                .build();

    /** Histogram metric. */
    private static final DiagramMetric HISTOGRAM_METRIC = DiagramMetric.newBuilder()
                                                                       .addComponents( FunctionFactory.HISTOGRAM_BINS )
                                                                       .addComponents( FunctionFactory.HISTOGRAM_COUNT )
                                                                       .setName( MetricName.HISTOGRAM )
                                                                       .build();

    /** Box plot metric. */
    private static final BoxplotMetric BOXPLOT_METRIC = BoxplotMetric.newBuilder()
                                                                     .setName( MetricName.BOX_PLOT )
                                                                     .setLinkedValueType( BoxplotMetric.LinkedValueType.NONE )
                                                                     .setQuantileValueType( BoxplotMetric.QuantileValueType.STATISTIC )
                                                                     .addAllQuantiles( FunctionFactory.DEFAULT_PROBABILITIES )
                                                                     .build();

    /** Function for rounding the errors. */
    private static final DoubleUnaryOperator ROUNDER = Slicer.rounder( 8 );

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
     * Return a function that computes the minimum value in a vector of doubles.
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
     * Return a function that computes the maximum value in a vector of doubles.
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
            if ( !FunctionFactory.isSorted()
                                 .test( samples ) )
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

    public static ScalarSummaryStatisticFunction quantileForSamplingUncertainty( double probability )
    {
        ToDoubleFunction<double[]> quantile = FunctionFactory.quantile( probability );
        SummaryStatistic statistic = SummaryStatistic.newBuilder()
                                                     .setStatistic( SummaryStatistic.StatisticName.QUANTILE )
                                                     .setProbability( probability )
                                                     .setDimension( SummaryStatistic.StatisticDimension.RESAMPLED )
                                                     .build();
        return new ScalarSummaryStatisticFunction( statistic, quantile );
    }

    /**
     * Returns a function that checks for sortedness of a double array. The function has O(N) complexity. By convention,
     * {@link Double#NaN} is considered the largest possible value.
     *
     * @return a function that checks for sortedness
     */
    public static Predicate<double[]> isSorted()
    {
        return check ->
        {
            Objects.requireNonNull( check );

            for ( int i = 0; i < check.length - 1; i++ )
            {
                if ( check[i] > check[i + 1]
                     || ( Double.isNaN( check[i] )  // NaN is always the biggest value
                          && !Double.isNaN( check[i + 1] ) ) )
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
     * @throws IllegalArgumentException if the parameters contain unexpected information
     * @throws NullPointerException if the parameters is null
     */
    public static DiagramSummaryStatisticFunction histogram( SummaryStatistic parameters )
    {
        Objects.requireNonNull( parameters );

        // Validate the parameters
        if ( parameters.getHistogramBins() < 2 )
        {
            throw new IllegalArgumentException( "Expected at least two bins for ths histogram." );
        }

        if ( parameters.getStatistic() != SummaryStatistic.StatisticName.HISTOGRAM )
        {
            throw new IllegalArgumentException( "Expected parameters for a histogram, but received parameters for a : "
                                                + parameters.getStatistic() );
        }

        int bins = parameters.getHistogramBins();

        BiFunction<Map<SummaryStatisticComponentName, String>, double[], DiagramStatistic> f = ( p, d ) ->
        {
            Objects.requireNonNull( p.get( SummaryStatisticComponentName.METRIC_NAME ),
                                    "Cannot create a histogram without a metric name." );
            Objects.requireNonNull( p.get( SummaryStatisticComponentName.METRIC_UNIT ),
                                    "Cannot create a histogram without a metric unit." );

            String metricNameString = p.get( SummaryStatisticComponentName.METRIC_NAME );
            String componentName = p.get( SummaryStatisticComponentName.METRIC_COMPONENT_NAME );

            MetricConstants metricNameEnum = MetricConstants.valueOf( metricNameString );
            MetricName metricName = metricNameEnum.getCanonicalName();

            String unitName = p.get( SummaryStatisticComponentName.METRIC_UNIT );

            DiagramMetric.DiagramMetricComponent domainAxis = FunctionFactory.HISTOGRAM_BINS.toBuilder()
                                                                                            .setUnits( unitName )
                                                                                            .build();

            DiagramMetric.Builder metric = FunctionFactory.HISTOGRAM_METRIC.toBuilder()
                                                                           .setStatisticUnits( unitName )
                                                                           .setStatisticName( metricName )
                                                                           .setStatisticMinimum( metricNameEnum.getMinimum() )
                                                                           .setStatisticMaximum( metricNameEnum.getMaximum() )
                                                                           .setStatisticOptimum( metricNameEnum.getOptimum() )
                                                                           .setComponents( 0, domainAxis );

            if ( Objects.nonNull( componentName ) )
            {
                MetricName componentNameEnum = MetricName.valueOf( componentName );
                metric.setStatisticComponentName( componentNameEnum );
            }

            EmpiricalDistribution empirical = new EmpiricalDistribution( bins );
            empirical.load( d );

            List<Double> binUpperBounds = Arrays.stream( empirical.getUpperBounds() )
                                                // Remove infinite bounds
                                                .map( x -> Double.isInfinite( x ) ? Double.NaN : x )
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
                                                              .addAllValues( binUpperBounds )
                                                              .build();

            DiagramStatistic.DiagramStatisticComponent rangeStatistic =
                    DiagramStatistic.DiagramStatisticComponent.newBuilder()
                                                              .setMetric( FunctionFactory.HISTOGRAM_COUNT )
                                                              .addAllValues( binCounts )
                                                              .build();

            return DiagramStatistic.newBuilder()
                                   .addStatistics( domainStatistic )
                                   .addStatistics( rangeStatistic )
                                   .setMetric( metric )
                                   .build();
        };

        return new DiagramSummaryStatisticFunction( parameters, f );
    }

    /**
     * Returns a function that calculates a box plot.
     * @param parameters the box plot parameters
     * @return the box plot function#
     * @throws IllegalArgumentException if the parameters contain unexpected information
     * @throws NullPointerException if the parameters is null
     */
    public static BoxplotSummaryStatisticFunction boxplot( SummaryStatistic parameters )
    {
        Objects.requireNonNull( parameters );

        // Validate the parameters
        if ( parameters.getStatistic() != SummaryStatistic.StatisticName.BOX_PLOT )
        {
            throw new IllegalArgumentException( "Expected parameters for a box plot, but received parameters for a : "
                                                + parameters.getStatistic() );
        }

        BiFunction<Map<SummaryStatisticComponentName, String>, double[], BoxplotStatistic> f = ( p, d ) ->
        {
            Objects.requireNonNull( p.get( SummaryStatisticComponentName.METRIC_NAME ),
                                    "Cannot create a box plot without a metric name." );
            Objects.requireNonNull( p.get( SummaryStatisticComponentName.METRIC_UNIT ),
                                    "Cannot create a box plot without a metric unit." );

            String metricName = p.get( SummaryStatisticComponentName.METRIC_NAME );
            MetricConstants subjectMetricConstant = MetricConstants.valueOf( metricName );
            MetricName subjectMetric = subjectMetricConstant.getCanonicalName();
            String componentName = p.get( SummaryStatisticComponentName.METRIC_COMPONENT_NAME );
            String unitName = p.get( SummaryStatisticComponentName.METRIC_UNIT );

            BoxplotMetric.Builder metric = BOXPLOT_METRIC.toBuilder()
                                                         .setStatisticName( subjectMetric )
                                                         .setMinimum( subjectMetricConstant.getMinimum() )
                                                         .setMaximum( subjectMetricConstant.getMaximum() )
                                                         .setOptimum( subjectMetricConstant.getOptimum() )
                                                         .setUnits( unitName );

            if ( Objects.nonNull( componentName ) )
            {
                MetricName componentNameEnum = MetricName.valueOf( componentName );
                metric.setStatisticComponentName( componentNameEnum );
            }

            // Compute the quantiles at a rounded precision
            List<Double> box = FunctionFactory.DEFAULT_PROBABILITIES.stream()
                                                                    .mapToDouble( Double::doubleValue )
                                                                    .map( pr -> FunctionFactory.quantile( pr )
                                                                                               .applyAsDouble( d ) )
                                                                    .map( FunctionFactory.ROUNDER )
                                                                    .boxed()
                                                                    .toList();

            return BoxplotStatistic.newBuilder()
                                   .setMetric( metric )
                                   .addStatistics( BoxplotStatistic.Box.newBuilder()
                                                                       .addAllQuantiles( box ) )
                                   .build();
        };

        return new BoxplotSummaryStatisticFunction( parameters, f );
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

    public static ToDoubleFunction<double[]> ofScalarSummaryStatistic( MetricConstants statistic )
    {
        Objects.requireNonNull( statistic );

        // Lazy build the map
        FunctionFactory.buildStatisticsMap();

        if ( !STATISTICS.containsKey( statistic ) )
        {
            throw new IllegalArgumentException( "The statistic '" + statistic
                                                + "' is not a recognized statistic "
                                                + "in this context." );
        }

        return STATISTICS.get( statistic );
    }

    /**
     * Returns a {@link ScalarSummaryStatisticFunction} from a {@link SummaryStatistic}.
     *
     * @param statistic the statistic
     * @return the statistic calculator
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the statistic is not a valid summary statistic in this context
     */

    public static ScalarSummaryStatisticFunction ofScalarSummaryStatistic( SummaryStatistic statistic )
    {
        return FunctionFactory.ofScalarSummaryStatistic( statistic, 0 );
    }

    /**
     * Returns a {@link ScalarSummaryStatisticFunction} from a {@link SummaryStatistic}.
     *
     * @param statistic the statistic
     * @param minimumSampleSize the minimum sample size
     * @return the statistic calculator
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the statistic is not a valid summary statistic in this context
     */

    public static ScalarSummaryStatisticFunction ofScalarSummaryStatistic( SummaryStatistic statistic,
                                                                           int minimumSampleSize )
    {
        Objects.requireNonNull( statistic );

        if ( statistic.getStatistic() == SummaryStatistic.StatisticName.QUANTILE )
        {
            return new ScalarSummaryStatisticFunction( statistic,
                                                       FunctionFactory.quantile( statistic.getProbability() ) );
        }

        MetricConstants name = MetricConstants.valueOf( statistic.getStatistic()
                                                                 .name() );

        // Filter missing values and apply the summary statistic
        ToDoubleFunction<double[]> summaryStatistic = FunctionFactory.ofScalarSummaryStatistic( name );
        ToDoubleFunction<double[]> calculator = array ->
        {
            double[] filtered = Arrays.stream( array )
                                      .filter( MissingValues::isNotMissingValue )
                                      .sorted()
                                      .toArray();

            // Insufficient samples?
            if ( filtered.length < minimumSampleSize )
            {
                return MissingValues.DOUBLE;
            }

            return summaryStatistic.applyAsDouble( filtered );
        };

        return new ScalarSummaryStatisticFunction( statistic, calculator );
    }

    /**
     * Returns a {@link DiagramSummaryStatisticFunction} from a {@link SummaryStatistic}.
     *
     * @param statistic the statistic
     * @return the statistic calculator
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the statistic is not a valid summary statistic in this context
     */

    public static DiagramSummaryStatisticFunction ofDiagramSummaryStatistic( SummaryStatistic statistic )
    {
        Objects.requireNonNull( statistic );

        if ( statistic.getStatistic() != SummaryStatistic.StatisticName.HISTOGRAM )
        {
            throw new IllegalArgumentException( "Unsupported diagram statistic: " + statistic.getStatistic() + "." );
        }

        return FunctionFactory.histogram( statistic );
    }

    /**
     * Returns a {@link BoxplotSummaryStatisticFunction} from a {@link SummaryStatistic}.
     *
     * @param statistic the statistic
     * @return the statistic calculator
     * @throws NullPointerException if the input is null
     * @throws IllegalArgumentException if the statistic is not a valid summary statistic in this context
     */

    public static BoxplotSummaryStatisticFunction ofBoxplotSummaryStatistic( SummaryStatistic statistic )
    {
        Objects.requireNonNull( statistic );

        if ( statistic.getStatistic() != SummaryStatistic.StatisticName.BOX_PLOT )
        {
            throw new IllegalArgumentException( "Unsupported box plot statistic: " + statistic.getStatistic() + "." );
        }

        return FunctionFactory.boxplot( statistic );
    }

    /**
     * Translates durations to decimal durations in prescribed units.
     * @param durations the durations
     * @param units the units
     * @return the decimal durations
     * @throws NullPointerException if any input is null
     */
    public static double[] ofDecimalDurations( Duration[] durations, ChronoUnit units )
    {
        Objects.requireNonNull( durations );
        Objects.requireNonNull( units );

        return Arrays.stream( durations )
                     .mapToDouble( n -> TimeSeriesSlicer.durationToDecimalMilliPrecision( n, units ) )
                     .toArray();
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
            double[] input = FunctionFactory.ofDecimalDurations( durations, ChronoUnit.MILLIS );

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
    public static BiFunction<Map<SummaryStatisticComponentName, String>, Duration[],
            DiagramStatistic> ofDurationDiagramFromUnivariateFunction( DiagramSummaryStatisticFunction function,
                                                                       ChronoUnit units )
    {
        // Create a function that operates on durations
        return ( p, d ) ->
        {
            double[] decimalDurations = FunctionFactory.ofDecimalDurations( d, units );

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
