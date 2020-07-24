package wres.datamodel;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.SampleDataBasic.SampleDataBasicBuilder;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.statistics.ScoreStatistic;
import wres.datamodel.statistics.ScoreStatistic.ScoreComponent;
import wres.datamodel.statistics.Statistic;
import wres.datamodel.thresholds.OneOrTwoThresholds;
import wres.datamodel.thresholds.ThresholdOuter;
import wres.datamodel.thresholds.ThresholdConstants.ThresholdType;
import wres.datamodel.time.TimeWindowOuter;
import wres.datamodel.time.TimeSeriesSlicer;

/**
 * A utility class for slicing/dicing and transforming datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @see    TimeSeriesSlicer
 */

public final class Slicer
{

    /**
     * Logger.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger( Slicer.class );

    /**
     * Null input error message.
     */
    public static final String NULL_INPUT_EXCEPTION = "Specify a non-null input.";

    /**
     * Null mapper function error message.
     */

    public static final String NULL_MAPPER_EXCEPTION = "Specify a non-null function to map the input to an output.";

    /**
     * Failure to supply a non-null predicate.
     */

    private static final String NULL_PREDICATE_EXCEPTION = "Specify a non-null predicate.";

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

        return pair -> predicate.test( pair.getLeft() ) && predicate.test( pair.getRight() );
    }

    /**
     * <p>Composes the input predicate as applying to the either the left side or to the right side of a pair.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     * @throws NullPointerException if the input is null
     */

    public static Predicate<Pair<Double, Double>> leftOrRight( DoublePredicate predicate )
    {
        Objects.requireNonNull( predicate, "Specify non-null input when slicing by left or right." );

        return pair -> predicate.test( pair.getLeft() ) || predicate.test( pair.getRight() );
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

        return pair -> Arrays.stream( pair.getRight().getMembers() ).allMatch( predicate );
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

        return pair -> Arrays.stream( pair.getRight().getMembers() ).anyMatch( predicate );
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
                       && Arrays.stream( pair.getRight().getMembers() ).allMatch( predicate );
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

        return pair -> predicate.test( transformer.applyAsDouble( pair.getRight().getMembers() ) );
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

    public static UnaryOperator<Pair<Double, Ensemble>>
            leftAndEachOfRight( DoublePredicate predicate )
    {
        return pair -> {
            Pair<Double, Ensemble> returnMe = null;

            //Left meets condition
            if ( predicate.test( pair.getLeft() ) )
            {
                double[] filtered = Arrays.stream( pair.getRight().getMembers() )
                                          .filter( predicate )
                                          .toArray();

                //One or more of right meets condition
                if ( filtered.length > 0 )
                {
                    // Labels? 
                    if ( pair.getRight().getLabels().isPresent() )
                    {
                        returnMe = Pair.of( pair.getLeft(),
                                            Ensemble.of( filtered, pair.getRight().getLabels().get() ) );
                    }
                    else
                    {
                        returnMe = Pair.of( pair.getLeft(), Ensemble.of( filtered ) );
                    }
                }
            }
            return returnMe;
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

    public static <T> double[] getLeftSide( SampleData<Pair<Double, T>> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.getRawData().stream().mapToDouble( Pair::getLeft ).toArray();
    }

    /**
     * Returns the right side of the input as a primitive array of doubles.
     * 
     * @param <T> the data type
     * @param input the input pairs
     * @return the right side
     * @throws NullPointerException if the input is null
     */

    public static <T> double[] getRightSide( SampleData<Pair<T, Double>> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.getRawData().stream().mapToDouble( Pair::getRight ).toArray();
    }

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * Does not modify the metadata associated with the input.
     * 
     * @param <T> the type of data
     * @param input the data to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws NullPointerException if either the input or condition is null
     */

    public static <T> SampleData<T> filter( SampleData<T> input,
                                            Predicate<T> condition,
                                            DoublePredicate applyToClimatology )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( condition, NULL_PREDICATE_EXCEPTION );

        SampleDataBasicBuilder<T> builder = new SampleDataBasicBuilder<>();

        List<T> mainPairs = input.getRawData();
        List<T> mainPairsSubset =
                mainPairs.stream().filter( condition ).collect( Collectors.toList() );

        builder.addData( mainPairsSubset ).setMetadata( input.getMetadata() );

        //Filter climatology as required
        if ( input.hasClimatology() )
        {
            VectorOfDoubles climatology = input.getClimatology();

            if ( Objects.nonNull( applyToClimatology ) )
            {
                climatology = Slicer.filter( input.getClimatology(), applyToClimatology );
            }

            builder.setClimatology( climatology );
        }

        //Filter baseline as required
        if ( input.hasBaseline() )
        {
            SampleData<T> baseline = input.getBaselineData();
            List<T> basePairs = baseline.getRawData();
            List<T> basePairsSubset =
                    basePairs.stream().filter( condition ).collect( Collectors.toList() );

            builder.addDataForBaseline( basePairsSubset ).setMetadataForBaseline( baseline.getMetadata() );
        }

        return builder.build();
    }

    /**
     * <p>Returns a subset of metric outputs whose {@link SampleMetadata} matches the supplied predicate. For 
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

        return outputs.stream().filter( predicate ).collect( Collectors.toUnmodifiableList() );
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
     * @param outputs the list of outputs
     * @param mapper the mapper function that discovers the type of interest
     * @return the unique instances of a given type associated with the output
     * @throws NullPointerException if the input list is null or the mapper is null
     */

    public static <S extends Statistic<?>, T extends Object> SortedSet<T> discover( List<S> outputs,
                                                                                    Function<S, T> mapper )
    {
        Objects.requireNonNull( outputs, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( mapper, NULL_INPUT_EXCEPTION );

        return Collections.unmodifiableSortedSet( outputs.stream()
                                                         .map( mapper )
                                                         .filter( Objects::nonNull )
                                                         .collect( Collectors.toCollection( TreeSet::new ) ) );
    }

    /**
     * <p>Convenience method that returns the metric output in the store whose identifier matches the 
     * input. This is equivalent to:</p>
     * 
     * <p><code>Slicer.filter( out, meta {@literal ->} meta.getMetricID() == metricIdentifier )</code></p>
     * 
     * @param <T> the metric output type
     * @param outputs the list of outputs
     * @param metricIdentifier the metric identifier                     
     * @throws NullPointerException if the input list is null or the identifier is null
     * @return the first available output that matches the input identifier or null if no such output is available
     */

    public static <T extends Statistic<?>> List<T> filter( List<T> outputs,
                                                           MetricConstants metricIdentifier )
    {
        Objects.requireNonNull( outputs, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( metricIdentifier, NULL_INPUT_EXCEPTION );
        
        return outputs.stream()
                      .filter( next -> metricIdentifier == next.getMetricName() )
                      .collect( Collectors.toUnmodifiableList() );
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

    public static Map<Integer, List<Pair<Double, Ensemble>>>
            filterByRightSize( List<Pair<Double, Ensemble>> input )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        return input.stream().collect( Collectors.groupingBy( pair -> pair.getRight().size() ) );
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
     * Transforms the input type to another type.
     * 
     * @param <S> the input type
     * @param <T> the output type
     * @param input the input
     * @param transformer the transformer
     * @return the transformed type
     * @throws NullPointerException if either input is null
     */

    public static <S, T> SampleData<T> transform( SampleData<S> input, Function<S, T> transformer )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( transformer, NULL_MAPPER_EXCEPTION );

        SampleDataBasicBuilder<T> builder = new SampleDataBasicBuilder<>();

        builder.setClimatology( input.getClimatology() )
               .setMetadata( input.getMetadata() );

        // Add the main series
        for ( S next : input.getRawData() )
        {
            T transformed = transformer.apply( next );
            if ( Objects.nonNull( transformed ) )
            {
                builder.addData( transformed );
            }
        }

        // Add the baseline series if available
        if ( input.hasBaseline() )
        {
            SampleData<S> baseline = input.getBaselineData();

            for ( S next : baseline.getRawData() )
            {
                T transformed = transformer.apply( next );
                if ( Objects.nonNull( transformed ) )
                {
                    builder.addDataForBaseline( transformed );
                }
            }

            builder.setMetadataForBaseline( baseline.getMetadata() );
        }

        return builder.build();
    }

    /**
     * Converts an ensemble pair to a pair that contains the probabilities that a discrete event occurs according to 
     * the left side and the right side, respectively. The event is represented by a {@link ThresholdOuter}.
     * 
     * @param pair the pair to transform
     * @param threshold the threshold
     * @return the transformed pair
     * @throws NullPointerException if either input is null
     */

    public static Pair<Probability, Probability> toDiscreteProbabilityPair( Pair<Double, Ensemble> pair,
                                                                            ThresholdOuter threshold )
    {
        Objects.requireNonNull( pair, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( threshold, NULL_INPUT_EXCEPTION );

        double rhs = Arrays.stream( pair.getRight().getMembers() )
                           .map( a -> threshold.test( a ) ? 1 : 0 )
                           .average()
                           .getAsDouble();

        return Pair.of( threshold.test( pair.getLeft() ) ? Probability.ONE : Probability.ZERO, Probability.of( rhs ) );
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
            if ( sorted.length == 0 )
            {
                return Double.NaN;
            }
            //Single item
            if ( sorted.length == 1 )
            {
                return sorted[0];
            }

            //Estimate the position
            double pos = probability * ( sorted.length + 1.0 );
            //Lower bound
            if ( pos < 1.0 )
            {
                return sorted[0];
            }
            //Upper bound
            else if ( pos >= sorted.length )
            {
                return sorted[sorted.length - 1];
            }
            //Contained: use linear interpolation
            else
            {
                double floorPos = Math.floor( pos );
                double dif = pos - floorPos;
                int intPos = (int) floorPos;
                double lower = sorted[intPos - 1];
                double upper = sorted[intPos];
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

        if ( threshold.getType() != ThresholdType.PROBABILITY_ONLY )
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
                                                  threshold.getDataType(),
                                                  threshold.getLabel(),
                                                  threshold.getUnits() );
        }
        DoubleUnaryOperator qF = Slicer.getQuantileFunction( sorted );
        Double first = qF.applyAsDouble( threshold.getProbabilities().first() );

        if ( Objects.nonNull( digits ) )
        {
            first = Slicer.rounder().apply( first, digits );
        }
        Double second = null;

        if ( threshold.hasBetweenCondition() )
        {
            second = qF.applyAsDouble( threshold.getProbabilities().second() );
            if ( Objects.nonNull( digits ) )
            {
                second = Slicer.rounder().apply( second, digits );
            }
        }

        return ThresholdOuter.ofQuantileThreshold( OneOrTwoDoubles.of( first, second ),
                                              threshold.getProbabilities(),
                                              threshold.getOperator(),
                                              threshold.getDataType(),
                                              threshold.getLabel(),
                                              threshold.getUnits() );
    }

    /**
     * Filters a {@link VectorOfDoubles}, returning a subset whose elements meet the condition.
     * 
     * @param input the input
     * @param condition the condition
     * @return the filtered vector
     */

    public static VectorOfDoubles filter( VectorOfDoubles input, DoublePredicate condition )
    {
        Objects.requireNonNull( input, NULL_INPUT_EXCEPTION );

        Objects.requireNonNull( input, NULL_PREDICATE_EXCEPTION );

        double[] filtered = Arrays.stream( input.getDoubles() )
                                  .filter( condition )
                                  .toArray();

        return VectorOfDoubles.of( filtered );
    }

    /**
     * Rounds the input to the prescribed number of decimal places using {@link BigDecimal#ROUND_HALF_UP}.
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
     * Rounds the input to the prescribed number of decimal places using {@link BigDecimal#ROUND_HALF_UP}.
     * 
     * @return a function that rounds to a prescribed number of decimal places
     */

    private static BiFunction<Double, Integer, Double> rounder()
    {
        return ( input, digits ) -> {
            BigDecimal bd = new BigDecimal( Double.toString( input ) ); //Always use String constructor
            bd = bd.setScale( digits, RoundingMode.HALF_UP );
            return bd.doubleValue();
        };
    }

    /**
     * Hidden constructor.
     */

    private Slicer()
    {
    }

}
