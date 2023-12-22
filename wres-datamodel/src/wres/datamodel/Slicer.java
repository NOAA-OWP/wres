package wres.datamodel;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.Ensemble.Labels;
import wres.config.MetricConstants;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.ScoreStatistic.ScoreComponent;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.space.Feature;

import wres.config.yaml.components.ThresholdType;
import wres.statistics.generated.Statistics;

/**
 * A utility class for slicing/dicing and transforming datasets.
 *
 * @author James Brown
 * @see    TimeSeriesSlicer
 * @see    PoolSlicer
 */

public final class Slicer
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( Slicer.class );

    /** Null input error message. */
    private static final String NULL_INPUT_EXCEPTION = "Specify a non-null input.";

    /** Failure to supply a non-null predicate. */
    private static final String NULL_PREDICATE_EXCEPTION = "Specify a non-null predicate.";

    /** Median function. */
    private static final Median MEDIAN = new Median();

    /**
     * <p>Composes the input predicate as applying to the left side of a pair. 
     *
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Double>> left( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left." );

        return pair -> predicate.test( pair.getLeft() );
    }

    /**
     * <p>Composes the input predicate as applying to the right side of a pair.
     *
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Double>> right( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by right." );

        return pair -> predicate.test( pair.getRight() );
    }

    /**
     * <p>Composes the input predicate as applying to the both the left and right sides of a pair.
     *
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Double>> leftAndRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left and right." );

        return pair -> predicate.test( pair.getLeft() )
                       && predicate.test( pair.getRight() );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair.
     *
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Ensemble>> leftVector( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left." );

        return pair -> predicate.test( pair.getLeft() );
    }

    /**
     * <p>Composes the input predicate as applying to all elements of the right side of a pair.
     *
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Ensemble>> allOfRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by all of right." );

        return pair -> Arrays.stream( pair.getRight()
                                          .getMembers() )
                             .allMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to one or more elements of the right side of a pair.
     *
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Ensemble>> anyOfRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by any of right." );

        return pair -> Arrays.stream( pair.getRight()
                                          .getMembers() )
                             .anyMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and all elements of the right side of a pair.
     *
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Ensemble>> leftAndAllOfRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left and all of right." );

        return pair -> predicate.test( pair.getLeft() )
                       && Arrays.stream( pair.getRight()
                                             .getMembers() )
                                .allMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and any element of the right side of a 
     * pair.
     *
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Ensemble>> leftAndAnyOfRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left and any of right." );

        return pair -> predicate.test( pair.getLeft() )
                       && Arrays.stream( pair.getRight().getMembers() ).anyMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the transformed value of the right side of a pair.
     *
     * @param predicate the input predicate
     * @param transformer the transformer
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Ensemble>> right( DoublePredicate predicate,
                                                           ToDoubleFunction<double[]> transformer )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by right." );

        Objects.requireNonNull( transformer, "Specify a non-null transformer when slicing by right." );

        return pair -> predicate.test( transformer.applyAsDouble( pair.getRight()
                                                                      .getMembers() ) );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and to the transformed value of the 
     * right side of a pair.
     *
     * @param predicate the input predicate
     * @param transformer the transformer
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Ensemble>> leftAndRight( DoublePredicate predicate,
                                                                  ToDoubleFunction<double[]> transformer )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left and right." );

        Objects.requireNonNull( transformer, "Specify a non-null transformer when slicing by left and right." );

        return pair -> predicate.test( pair.getLeft() )
                       && predicate.test( transformer.applyAsDouble( pair.getRight().getMembers() ) );
    }

    /**
     * A transformer that applies a predicate to the left and each of the right separately, returning a transformed
     * pair or null if the left and none of the right meet the condition.
     *
     * @param predicate the input predicate
     * @return a composed function
     */

    public static UnaryOperator<Pair<Double, Ensemble>> leftAndEachOfRight( DoublePredicate predicate )
    {
        return pair -> {
            // Left fails condition
            if ( !predicate.test( pair.getLeft() ) )
            {
                return null;
            }

            Ensemble ensemble = Slicer.eachOfRight( predicate )
                                      .apply( pair.getRight() );

            if ( ensemble.size() == 0 )
            {
                return null;
            }

            return Pair.of( pair.getLeft(), ensemble );
        };
    }

    /**
     * A transformer that applies a predicate to the left and each of the right separately, returning a transformed
     * pair or null if the left and none of the right meet the condition.
     *
     * @param predicate the input predicate
     * @return a composed function
     */

    public static UnaryOperator<Ensemble> eachOfRight( DoublePredicate predicate )
    {
        return ensemble -> {
            double[] members = ensemble.getMembers();
            Labels labels = ensemble.getLabels();
            String[] labelValues = labels.getLabels();
            List<Double> filteredMembers = new ArrayList<>();
            List<String> filteredLabels = new ArrayList<>();

            // Iterate and filter the members and corresponding labels
            for ( int i = 0; i < members.length; i++ )
            {
                if ( predicate.test( members[i] ) )
                {
                    filteredMembers.add( members[i] );

                    if ( ensemble.hasLabels() )
                    {
                        filteredLabels.add( labelValues[i] );
                    }
                }
            }

            // Optimization, nothing filtered
            if ( filteredMembers.size() == members.length )
            {
                return ensemble;
            }

            // Unbox
            double[] filteredMemberArray = filteredMembers.stream()
                                                          .mapToDouble( Double::doubleValue )
                                                          .toArray();

            String[] filteredLabelArray = filteredLabels.toArray( new String[0] );

            // One or more of right meets condition
            if ( filteredMemberArray.length > 0 )
            {
                Labels fLabels = Labels.of( filteredLabelArray );
                return Ensemble.of( filteredMemberArray, fLabels, ensemble.areSortedMembersCached() );
            }
            else
            {
                return Ensemble.of();
            }
        };
    }

    /**
     * Returns the left side of the input as a primitive array of doubles.
     *
     * @param <T> the data type
     * @param input the input pairs
     * @return the left side
     * @throws NullPointerException if the input is null
     */

    public static <T> double[] getLeftSide( Pool<Pair<Double, T>> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.get().stream().mapToDouble( Pair::getLeft ).toArray();
    }

    /**
     * Returns the right side of the input as a primitive array of doubles.
     *
     * @param <T> the data type
     * @param input the input pairs
     * @return the right side
     * @throws NullPointerException if the input is null
     */

    public static <T> double[] getRightSide( Pool<Pair<T, Double>> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.get()
                    .stream()
                    .mapToDouble( Pair::getRight )
                    .toArray();
    }

    /**
     * <p>Returns a subset of metric outputs whose {@link PoolMetadata} matches the supplied predicate. For 
     * example, to filter by a particular {@link TimeWindowOuter} and {@link OneOrTwoThresholds} associated with the 
     * output metadata:</p>
     *
     * <p><code>Slicer.filter( list, a {@literal ->} a.getMetadata().getTimeWindow().equals( someWindow ) 
     *                      {@literal &&} a.getMetadata().getThresholds().equals( someThreshold ) );</code></p>
     *
     * @param <T> the output type
     * @param outputs the outputs to filter
     * @param predicate the predicate to use as a filter
     * @return a filtered list whose elements meet the predicate supplied
     * @throws NullPointerException if the input list is null or the predicate is null
     */

    public static <T extends Statistic<?>> List<T> filter( List<T> outputs,
                                                           Predicate<T> predicate )
    {
        Objects.requireNonNull( outputs, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( predicate, NULL_INPUT_EXCEPTION );

        return outputs.stream().filter( predicate ).toList();
    }

    /**
     * Removes the ensemble members that match the input labels.
     *
     * @param ensemble the ensemble to modify
     * @param labels the labels to remove
     * @return the modified ensemble
     * @throws IllegalArgumentException if the ensemble does not contain any labels
     * @throws NullPointerException if either input is null
     */

    public static Ensemble filter( Ensemble ensemble, String... labels )
    {
        Objects.requireNonNull( ensemble );
        Objects.requireNonNull( labels );

        if ( !ensemble.hasLabels() )
        {
            throw new IllegalArgumentException( "Cannot filter ensemble " + ensemble
                                                + " to remove labels because the "
                                                + "ensemble is not labelled." );
        }

        // No-op
        if ( labels.length == 0 )
        {
            return ensemble;
        }

        List<Double> newMembers = new ArrayList<>();
        List<String> newLabels = new ArrayList<>();

        double[] oldMembers = ensemble.getMembers();
        String[] oldLabels = ensemble.getLabels().getLabels();
        List<String> filterLabels = Arrays.asList( labels );

        // Iterate and filter the members and corresponding labels
        for ( int i = 0; i < oldMembers.length; i++ )
        {
            if ( !filterLabels.contains( oldLabels[i] ) )
            {
                newMembers.add( oldMembers[i] );
                newLabels.add( oldLabels[i] );
            }
        }

        // Unbox
        double[] filteredMemberArray = newMembers.stream()
                                                 .mapToDouble( Double::doubleValue )
                                                 .toArray();

        String[] filteredLabelArray = newLabels.toArray( new String[0] );

        return Ensemble.of( filteredMemberArray, Labels.of( filteredLabelArray ), ensemble.areSortedMembersCached() );
    }

    /**
     * <p>Convenience method that returns the metric output in the store whose identifier matches the 
     * input. This is equivalent to:</p>
     *
     * <p><code>Slicer.filter( out, meta {@literal ->} meta.getMetricID() == metricIdentifier )</code></p>
     *
     * @param <T> the metric output type
     * @param statistics the list of outputs
     * @param metricIdentifier the metric identifier                     
     * @throws NullPointerException if the input list is null or the identifier is null
     * @return the first available output that matches the input identifier or null if no such output is available
     */

    public static <T extends Statistic<?>> List<T> filter( List<T> statistics,
                                                           MetricConstants metricIdentifier )
    {
        Objects.requireNonNull( statistics, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( metricIdentifier, NULL_INPUT_EXCEPTION );

        return statistics.stream()
                         .filter( next -> metricIdentifier == next.getMetricName() )
                         .toList();
    }

    /**
     * Returns as many lists of ensemble pairs as groups of atomic pairs in the input with an equal number of elements,
     * i.e. each list of ensemble pairs in the returned result has an equal number of elements, internally, and a
     * different number of elements than all other subsets of pairs. The subsets are returned in a map, indexed by the
     * number of elements on the right side.
     *
     * @param input a list of ensemble pairs to slice
     * @return as many subsets of ensemble pairs as groups of pairs in the input of equal size
     * @throws NullPointerException if the input is null
     */

    public static Map<Integer, List<Pair<Double, Ensemble>>> filterByRightSize( List<Pair<Double, Ensemble>> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.stream().collect( Collectors.groupingBy( pair -> pair.getRight().size() ) );
    }

    /**
     * <p>Discovers the unique instances of a given type of statistic. The mapper function identifies the type to
     * discover. For example, to discover the unique thresholds contained in the list of outputs:</p>
     *
     * <p><code>Slicer.discover( outputs, next {@literal ->}
     *                                         next.getMetadata().getThresholds() );</code></p>
     *
     * <p>To discover the unique pairs of lead times in the list of outputs:</p>
     *
     * <p><code>Slicer.discover( outputs, next {@literal ->}
     * Pair.of( next.getMetadata().getTimeWindow().getEarliestLeadTime(),
     * next.getMetadata().getTimeWindow().getLatestLeadTime() );</code></p>
     *
     * <p>Returns the empty set if no elements are mapped.</p>
     *
     * @param <S> the metric output type
     * @param <T> the type of information required about the output
     * @param statistics the list of outputs
     * @param mapper the mapper function that discovers the type of interest
     * @return the unique instances of a given type associated with the output
     * @throws NullPointerException if the input list is null or the mapper is null
     */

    public static <S extends Statistic<?>, T> SortedSet<T> discover( List<S> statistics,
                                                                     Function<S, T> mapper )
    {
        Objects.requireNonNull( statistics, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_INPUT_EXCEPTION );

        return Collections.unmodifiableSortedSet( statistics.stream()
                                                            .map( mapper )
                                                            .filter( Objects::nonNull )
                                                            .collect( Collectors.toCollection( TreeSet::new ) ) );
    }

    /**
     * <p>Sorts the statistics by time window, then threshold, returning a new list with sorted statistics.
     *
     * @param <T> the type of statistics
     * @param statistics the statistics to sort    
     * @throws NullPointerException if the input is null
     * @return the sorted statistics
     */

    public static <T extends Statistic<?>> List<T> sortByTimeWindowAndThreshold( List<T> statistics )
    {
        Objects.requireNonNull( statistics, NULL_INPUT_EXCEPTION );

        // Null-friendly comparator on time window
        Comparator<T> twComparator = ( a, b ) -> {
            TimeWindowOuter first = null;
            TimeWindowOuter second = null;

            if ( Objects.nonNull( a )
                 && Objects.nonNull( a.getPoolMetadata() )
                 && Objects.nonNull( a.getPoolMetadata()
                                      .getTimeWindow() ) )
            {
                first = a.getPoolMetadata()
                         .getTimeWindow();
            }

            if ( Objects.nonNull( b )
                 && Objects.nonNull( b.getPoolMetadata() )
                 && Objects.nonNull( b.getPoolMetadata()
                                      .getTimeWindow() ) )
            {
                second = b.getPoolMetadata()
                          .getTimeWindow();
            }

            return ObjectUtils.compare( first, second );
        };

        // Null-friendly comparator on threshold
        Comparator<T> trComparator = ( a, b ) ->
        {
            OneOrTwoThresholds first = null;
            OneOrTwoThresholds second = null;

            if ( Objects.nonNull( a )
                 && Objects.nonNull( a.getPoolMetadata() )
                 && Objects.nonNull( a.getPoolMetadata()
                                      .getThresholds() ) )
            {
                first = a.getPoolMetadata()
                         .getThresholds();
            }

            if ( Objects.nonNull( b )
                 && Objects.nonNull( b.getPoolMetadata() )
                 && Objects.nonNull( b.getPoolMetadata()
                                      .getThresholds() ) )
            {
                second = b.getPoolMetadata()
                          .getThresholds();
            }

            return ObjectUtils.compare( first, second );
        };

        // Combined comparator
        Comparator<T> compare = twComparator.thenComparing( trComparator );

        List<T> returnMe = new ArrayList<>( statistics );
        returnMe.sort( compare );

        return Collections.unmodifiableList( returnMe );
    }

    /**
     * Returns a map of {@link ScoreComponent} for each component in the input map of {@link ScoreStatistic}. The 
     * slices are mapped to their {@link MetricConstants} component identifier.
     *
     * @param <S>  the score component type
     * @param <T> the score type
     * @param input the input map
     * @return the input map sliced by component identifier
     * @throws NullPointerException if the input is null
     */

    public static <S extends ScoreComponent<?>, T extends ScoreStatistic<?, S>> Map<MetricConstants, List<S>>
    filterByMetricComponent( List<T> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Map<MetricConstants, List<S>> returnMe = new EnumMap<>( MetricConstants.class );

        // Loop the scores and components
        for ( T nextScore : input )
        {
            // Loop the entries
            for ( S nextComponent : nextScore )
            {
                List<S> listOfComponents = returnMe.get( nextComponent.getMetricName() );

                if ( Objects.isNull( listOfComponents ) )
                {
                    listOfComponents = new ArrayList<>();
                    returnMe.put( nextComponent.getMetricName(), listOfComponents );
                }

                listOfComponents.add( nextComponent );
            }
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Converts an ensemble pair to a pair that contains the probabilities that a discrete event occurs according to 
     * the left side and the right side, respectively. The event is represented by a {@link ThresholdOuter}.
     *
     * @param pair the pair to transform
     * @param threshold the threshold
     * @return the transformed pair or null if no pair could be formed
     * @throws NullPointerException if either input is null
     */

    public static Pair<Probability, Probability> toDiscreteProbabilityPair( Pair<Double, Ensemble> pair,
                                                                            ThresholdOuter threshold )
    {
        Objects.requireNonNull( pair, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( threshold, NULL_INPUT_EXCEPTION );

        OptionalDouble rhs = Arrays.stream( pair.getRight().getMembers() )
                                   .map( a -> threshold.test( a ) ? 1 : 0 )
                                   .average();

        if ( rhs.isPresent() )
        {
            return Pair.of( threshold.test( pair.getLeft() ) ? Probability.ONE : Probability.ZERO,
                            Probability.of( rhs.getAsDouble() ) );
        }

        return null;
    }

    /**
     * Returns a function to compute a value from the sorted array that corresponds to the input non-exceedence 
     * probability. This method produces undefined results if the input array is unsorted. Corresponds to 
     * method 6 in the R function, <code>quantile{stats}</code>. This is largely equivalent to <code>PERCENTILE.EXC</code> 
     * function in Microsoft Excel, but the latter cannot compute the bounds or values close to the bounds: <a href=
     * "https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html">https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html</a>.
     * Also see: <a href=
     * "https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample">https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample</a>.
     *
     * @param sorted the sorted input array
     * @return the threshold
     * @throws NullPointerException if the input is null
     */

    public static DoubleUnaryOperator getQuantileFunction( double[] sorted )
    {
        return probability -> {
            if ( probability < 0 || probability > 1 )
            {
                throw new IllegalArgumentException( "The input probability is not within the unit interval: "
                                                    + probability );
            }

            // Remove any trailing NaN from the sorted array
            double[] sortedFinal = Slicer.stripNaNFromSortedArray( sorted );

            if ( sortedFinal.length == 0 )
            {
                return Double.NaN;
            }

            // Single item
            if ( sortedFinal.length == 1 )
            {
                return sortedFinal[0];
            }

            // Estimate the position
            double pos = probability * ( sortedFinal.length + 1.0 );

            //Lower bound
            if ( pos < 1.0 )
            {
                return sortedFinal[0];
            }

            // Upper bound
            else if ( pos >= sortedFinal.length )
            {
                return sortedFinal[sortedFinal.length - 1];
            }

            // Contained: use linear interpolation
            else
            {
                double floorPos = Math.floor( pos );
                double dif = pos - floorPos;
                int intPos = ( int ) floorPos;
                double lower = sortedFinal[intPos - 1];
                double upper = sortedFinal[intPos];
                return lower + dif * ( upper - lower );
            }
        };
    }

    /**
     * Returns a {@link ThresholdOuter} with quantiles defined from a prescribed {@link ThresholdOuter} with probabilities, 
     * where the quantiles are mapped using {@link #getQuantileFunction(double[])}. If the input is empty, returns
     * a threshold whose value is {@link Double#NaN}.
     *
     * @param sorted the sorted input array
     * @param threshold the probability threshold from which the quantile threshold is determined
     * @param digits an optional number of decimal places to which the threshold will be rounded up (may be null)
     * @return the probability threshold
     * @throws NullPointerException if either input is null
     */

    public static ThresholdOuter getQuantileFromProbability( ThresholdOuter threshold, double[] sorted, Integer digits )
    {
        Objects.requireNonNull( threshold, "Specify a non-null probability threshold." );

        Objects.requireNonNull( sorted, "Specify a non-null array of sorted values." );

        if ( threshold.getType() != ThresholdType.PROBABILITY )
        {
            throw new IllegalArgumentException( "The input threshold must be a probability threshold." );
        }

        if ( sorted.length == 0 )
        {
            // #65881
            LOGGER.debug( "Returning a default quantile because there were no measurements from which to determine a "
                          + "measured quantile." );

            return ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( Double.NaN ),
                                                       threshold.getProbabilities(),
                                                       threshold.getOperator(),
                                                       threshold.getOrientation(),
                                                       threshold.getLabel(),
                                                       threshold.getUnits() );
        }
        DoubleUnaryOperator qF = Slicer.getQuantileFunction( sorted );
        double first = qF.applyAsDouble( threshold.getProbabilities().first() );

        if ( Objects.nonNull( digits ) )
        {
            first = Slicer.rounder( digits )
                          .applyAsDouble( first );
        }
        Double second = null;

        if ( threshold.hasBetweenCondition() )
        {
            second = qF.applyAsDouble( threshold.getProbabilities().second() );
            if ( Objects.nonNull( digits ) )
            {
                second = Slicer.rounder( digits )
                               .applyAsDouble( second );
            }
        }

        return ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( first, second ),
                                                   threshold.getProbabilities(),
                                                   threshold.getOperator(),
                                                   threshold.getOrientation(),
                                                   threshold.getLabel(),
                                                   threshold.getUnits() );
    }

    /**
     * Filters a {@link Climatology}, returning a subset whose elements meet the condition.
     *
     * @param input the input
     * @param condition the condition
     * @return the filtered vector
     */

    public static Climatology filter( Climatology input, DoublePredicate condition )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( input, NULL_PREDICATE_EXCEPTION );


        Climatology.Builder builder = new Climatology.Builder();

        for ( Feature nextFeature : input.getFeatures() )
        {
            double[] climatology = input.get( nextFeature );
            double[] filtered = Arrays.stream( climatology )
                                      .filter( condition )
                                      .toArray();
            builder.addClimatology( nextFeature, filtered, input.getMeasurementUnit() );
        }

        return builder.build();
    }

    /**
     * Rounds the input to the prescribed number of decimal places using {@link RoundingMode#HALF_UP}.
     *
     * @param decimalPlaces the number of decimal places
     * @return a function that rounds to a prescribed number of decimal places
     */

    public static DoubleUnaryOperator rounder( int decimalPlaces )
    {
        BiFunction<Double, Integer, Double> round = Slicer.rounder();

        return in -> round.apply( in, decimalPlaces );
    }

    /**
     * Returns a map of statistics grouped by their {@link DatasetOrientation}.
     *
     * @param <T> the type of statistic
     * @param statistics the input list of statistics
     * @return the statistics grouped by context
     */

    public static <T extends Statistic<?>> Map<DatasetOrientation, List<T>> getGroupedStatistics( List<T> statistics )
    {
        Function<? super T, DatasetOrientation> classifier = statistic -> {
            if ( statistic.getPoolMetadata()
                          .getPool()
                          .getIsBaselinePool() )
            {
                return DatasetOrientation.BASELINE;
            }

            return DatasetOrientation.RIGHT;
        };

        Map<DatasetOrientation, List<T>> groups =
                statistics.stream()
                          .collect( Collectors.groupingBy( classifier ) );

        return Collections.unmodifiableMap( groups );
    }

    /**
     * Returns a map of statistics grouped by their {@link DatasetOrientation}.
     *
     * @param statistics the input list of statistics
     * @return the statistics grouped by context
     */

    public static Map<DatasetOrientation, List<Statistics>> getGroupedStatistics( Collection<Statistics> statistics )
    {
        // Split the statistics into two groups as there may be separate statistics for a baseline
        Function<? super Statistics, DatasetOrientation> classifier = statistic -> {
            if ( !statistic.hasPool()
                 && statistic.hasBaselinePool() )
            {
                return DatasetOrientation.BASELINE;
            }

            return DatasetOrientation.RIGHT;
        };

        return statistics.stream()
                         .collect( Collectors.groupingBy( classifier ) );
    }

    /**
     * Concatenates the input, appending from left to right.
     *
     * @param input the input
     * @return the concatenated input
     */

    public static VectorOfDoubles concatenate( VectorOfDoubles... input )
    {
        DoubleStream combined = DoubleStream.of();

        try
        {
            for ( VectorOfDoubles next : input )
            {
                DoubleStream nextStream = Arrays.stream( next.getDoubles() );
                combined = DoubleStream.concat( combined, nextStream );
            }

            return VectorOfDoubles.of( combined.toArray() );
        }
        finally
        {
            combined.close();
        }
    }

    /**
     * Creates an averaging function that converts an {@link Ensemble} to a single value.
     * @param ensembleAverageType the averaging type, not null
     * @return the transformer
     */
    public static ToDoubleFunction<Ensemble> getEnsembleAverageFunction( wres.statistics.generated.Pool.EnsembleAverageType ensembleAverageType )
    {
        Objects.requireNonNull( ensembleAverageType );

        if ( ensembleAverageType == wres.statistics.generated.Pool.EnsembleAverageType.MEDIAN )
        {
            return ensemble -> Slicer.MEDIAN.evaluate( ensemble.getMembers() );
        }
        else
        {
            return ensemble -> Arrays.stream( ensemble.getMembers() )
                                     .average()
                                     .orElse( MissingValues.DOUBLE );
        }
    }

    /**
     * Rounds the input to the prescribed number of decimal places using {@link RoundingMode#HALF_UP}.
     *
     * @return a function that rounds to a prescribed number of decimal places
     */

    private static BiFunction<Double, Integer, Double> rounder()
    {
        return ( input, digits ) -> {
            // #115230
            if ( !Double.isFinite( input ) )
            {
                return input;
            }
            BigDecimal bd = new BigDecimal( Double.toString( input ) ); //Always use String constructor
            bd = bd.setScale( digits, RoundingMode.HALF_UP );
            return bd.doubleValue();
        };
    }

    /**
     * Strips {@link Double#NaN} from the sorted input array where all such instances are trailing.
     * @param sorted the sorted input array with trailing {@link Double#NaN}, if any
     * @return the filtered array
     */

    private static double[] stripNaNFromSortedArray( double[] sorted )
    {
        if ( sorted.length == 0
             || !Double.isNaN( sorted[sorted.length - 1] ) )
        {
            return sorted;
        }

        int stop = sorted.length - 1;

        for ( int i = sorted.length - 2; i >= 0; i-- )
        {
            if ( !Double.isNaN( sorted[i] ) )
            {
                break;
            }
            stop = i;
        }
        double[] returnMe = new double[stop];
        System.arraycopy( sorted, 0, returnMe, 0, stop );
        return returnMe;
    }

    /**
     * Hidden constructor.
     */

    private Slicer()
    {
    }
}
