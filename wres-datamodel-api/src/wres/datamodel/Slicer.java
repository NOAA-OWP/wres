package wres.datamodel;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.DoublePredicate;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;

import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.ScoreOutput;

/**
 * A utility class for slicing/dicing and transforming datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface Slicer
{

    /**
     * <p>Composes the input predicate as applying to the left side of a pair. 
     * 
     * <p>Also see {@link #filter(SingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubles> left( DoublePredicate predicate )
    {
        return pair -> predicate.test( pair.getItemOne() );
    }

    /**
     * <p>Composes the input predicate as applying to the right side of a pair.
     * 
     * <p>Also see {@link #filter(SingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubles> right( DoublePredicate predicate )
    {
        return pair -> predicate.test( pair.getItemTwo() );
    }

    /**
     * <p>Composes the input predicate as applying to the both the left and right sides of a pair.
     * 
     * <p>Also see {@link #filter(SingleValuedPairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubles> leftAndRight( DoublePredicate predicate )
    {
        return pair -> predicate.test( pair.getItemOne() ) && predicate.test( pair.getItemTwo() );
    }

    /**
     * <p>Composes the input predicate as applying to the either the left side or to the right side of a pair.
     * 
     * <p>Also see {@link #filter(SingleValuedPairs, Predicate, DoublePredicate)}.
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubles> leftOrRight( DoublePredicate predicate )
    {
        return pair -> predicate.test( pair.getItemOne() ) || predicate.test( pair.getItemTwo() );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubleAndVectorOfDoubles> leftVector( DoublePredicate predicate )
    {
        return pair -> predicate.test( pair.getItemOne() );
    }

    /**
     * <p>Composes the input predicate as applying to all elements of the right side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubleAndVectorOfDoubles> allOfRight( DoublePredicate predicate )
    {
        return pair -> Arrays.stream( pair.getItemTwo() ).allMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to one or more elements of the right side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubleAndVectorOfDoubles> anyOfRight( DoublePredicate predicate )
    {
        return pair -> Arrays.stream( pair.getItemTwo() ).anyMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and all elements of the right side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubleAndVectorOfDoubles> leftAndAllOfRight( DoublePredicate predicate )
    {
        return pair -> predicate.test( pair.getItemOne() ) && Arrays.stream( pair.getItemTwo() ).allMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and any element of the right side of a pair.
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @return a composed predicate
     */

    static Predicate<PairOfDoubleAndVectorOfDoubles> leftAndAnyOfRight( DoublePredicate predicate )
    {
        return pair -> predicate.test( pair.getItemOne() ) && Arrays.stream( pair.getItemTwo() ).anyMatch( predicate );
    }

    /**
     * <p>Composes the input predicate as applying to the transformed value of the right side of a pair.</p>
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @param transformer the transformer
     * @return a composed predicate
     */

    static Predicate<PairOfDoubleAndVectorOfDoubles> right( DoublePredicate predicate,
                                                            ToDoubleFunction<double[]> transformer )
    {
        return pair -> predicate.test( transformer.applyAsDouble( pair.getItemTwo() ) );
    }

    /**
     * <p>Composes the input predicate as applying to the left side of a pair and to the transformed value of the 
     * right side of a pair.</p>
     * 
     * <p>Also see {@link #filter(EnsemblePairs, Predicate, DoublePredicate)}
     * 
     * @param predicate the input predicate
     * @param transformer the transformer
     * @return a composed predicate
     */

    static Predicate<PairOfDoubleAndVectorOfDoubles> leftAndRight( DoublePredicate predicate,
                                                                   ToDoubleFunction<double[]> transformer )
    {
        return pair -> predicate.test( pair.getItemOne() )
                       && predicate.test( transformer.applyAsDouble( pair.getItemTwo() ) );
    }

    /**
     * Loops over the {@link PairOfDoubles} in the input and returns <code>true</code> when a pair is encountered
     * for which the {@link Predicate} returns <code>true</code> for both sides of the pairing, false otherwise. 
     * 
     * @param pairs the input pairs
     * @param condition the predicate to apply
     * @return true if one or more inputs meet the predicate condition on both sides of the pairing, false otherwise
     * @throws NullPointerException if either input is null
     */

    static boolean hasOneOrMoreOf( List<PairOfDoubles> pairs, DoublePredicate condition )
    {
        Objects.requireNonNull( pairs, "Expected non-null pairs." );
        Objects.requireNonNull( condition, "Expected a non-null condition." );
        for ( PairOfDoubles next : pairs )
        {
            if ( condition.test( next.getItemOne() ) && condition.test( next.getItemTwo() ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Loops over the {@link PairOfDoubleAndVectorOfDoubles} in the input and returns <code>true</code> when a pair 
     * is encountered for which the {@link Predicate} returns <code>true</code> for the left side of the pairing and
     * for one or more values on the right side, false otherwise. 
     * 
     * @param pairs the input pairs
     * @param condition the predicate to apply
     * @return true if one or more inputs meet the predicate condition on both sides of the pairing, false otherwise
     * @throws NullPointerException if either input is null
     */

    static boolean hasOneOrMoreOfVectorRight( List<PairOfDoubleAndVectorOfDoubles> pairs, DoublePredicate condition )
    {
        Objects.requireNonNull( pairs, "Expected non-null pairs." );
        Objects.requireNonNull( condition, "Expected a non-null condition." );
        for ( PairOfDoubleAndVectorOfDoubles next : pairs )
        {
            if ( condition.test( next.getItemOne() ) && Arrays.stream( next.getItemTwo() ).anyMatch( condition ) )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a {@link Threshold} with quantiles defined from the prescribed {@link Threshold} with probabilities, 
     * where the quantiles are mapped using {@link #getQuantileFunction(double[])}.
     * 
     * @param sorted the sorted input array
     * @param threshold the probability threshold from which the quantile is determined
     * @return the quantile threshold
     */

    default Threshold getQuantileFromProbability( Threshold threshold, double[] sorted )
    {
        return this.getQuantileFromProbability( threshold, sorted, null );
    }

    /**
     * Returns the left side of {@link SingleValuedPairs#getData()} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the left side
     */

    double[] getLeftSide( SingleValuedPairs input );

    /**
     * Returns the right side of {@link SingleValuedPairs#getData()} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the right side
     */

    double[] getRightSide( SingleValuedPairs input );

    /**
     * Returns the left side of {@link EnsemblePairs#getData()} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the left side
     */

    double[] getLeftSide( EnsemblePairs input );

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * 
     * @param input the pairs to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws MetricInputSliceException if the slice contains no elements
     * @throws NullPointerException if either the input or condition is null
     */

    SingleValuedPairs filter( SingleValuedPairs input,
                              Predicate<PairOfDoubles> condition,
                              DoublePredicate applyToClimatology )
            throws MetricInputSliceException;

    /**
     * Returns the subset of pairs where the condition is met. Applies to both the main pairs and any baseline pairs.
     * 
     * @param input the pairs to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the subset of pairs that meet the condition
     * @throws MetricInputSliceException if the slice contains no elements
     * @throws NullPointerException if either the input or condition is null
     */

    EnsemblePairs filter( EnsemblePairs input,
                          Predicate<PairOfDoubleAndVectorOfDoubles> condition,
                          DoublePredicate applyToClimatology )
            throws MetricInputSliceException;

    /**
     * Filters {@link EnsemblePairs} by applying a mapper function to the input. This allows for fine-grain filtering
     * of specific elements of the right side of a pair. For example, filter all elements of the right side that 
     * correspond to {@link Double#isNaN()}.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to a new {@link EnsemblePairs}
     * @param applyToClimatology an optional filter for the climatology, may be null
     * @return the filtered {@link EnsemblePairs}
     * @throws MetricInputSliceException if the output could not be transformed
     * @throws NullPointerException if either the input or condition is null
     */

    EnsemblePairs filter( EnsemblePairs input,
                          Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubleAndVectorOfDoubles> mapper,
                          DoublePredicate applyToClimatology )
            throws MetricInputSliceException;

    /**
     * Returns as many lists of {@link PairOfDoubleAndVectorOfDoubles} as groups of atomic pairs in the input with an
     * equal number of elements. i.e. each list of {@link PairOfDoubleAndVectorOfDoubles} in the returned result has an
     * equal number of elements, internally, and a different number of elements than all other subsets of pairs. The
     * subsets are returned in a map, indexed by the number of elements on the right side.
     * 
     * @param input a list of {@link PairOfDoubleAndVectorOfDoubles} to slice
     * @return as many subsets of {@link PairOfDoubleAndVectorOfDoubles} as groups of pairs in the input of equal size
     */

    Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> filterByRightSize( List<PairOfDoubleAndVectorOfDoubles> input );

    /**
     * Returns a map of {@link ScoreOutput} for each component in the input map of {@link ScoreOutput}. The slices are 
     * mapped to their {@link MetricConstants} component identifier.
     * 
     * @param <T> the score component type
     * @param input the input map
     * @return the input map sliced by component identifier
     */

    <T extends ScoreOutput<?, T>> Map<MetricConstants, MetricOutputMapByTimeAndThreshold<T>>
            filterByMetricComponent( MetricOutputMapByTimeAndThreshold<T> input );

    /**
     * Produces a {@link List} of {@link PairOfDoubles} from a {@link List} of {@link PairOfDoubleAndVectorOfDoubles}
     * using a prescribed mapper function.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link SingleValuedPairs}
     * @return the {@link SingleValuedPairs}
     */

    List<PairOfDoubles> transform( List<PairOfDoubleAndVectorOfDoubles> input,
                                   Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper );

    /**
     * Produces {@link DichotomousPairs} from a {@link SingleValuedPairs} by applying a mapper function to the input.
     * 
     * @param input the {@link SingleValuedPairs} pairs
     * @param mapper the function that maps from {@link SingleValuedPairs} to {@link DichotomousPairs}
     * @return the {@link DichotomousPairs}
     */

    DichotomousPairs transform( SingleValuedPairs input, Function<PairOfDoubles, PairOfBooleans> mapper );

    /**
     * Produces {@link SingleValuedPairs} from a {@link EnsemblePairs} by applying a mapper function to the input.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link SingleValuedPairs} pairs
     * @return the {@link SingleValuedPairs}
     */

    SingleValuedPairs transform( EnsemblePairs input,
                                 Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper );

    /**
     * Produces {@link DiscreteProbabilityPairs} from a {@link EnsemblePairs} by applying a mapper function to the input
     * using a prescribed {@link Threshold}.
     * 
     * @param input the {@link EnsemblePairs}
     * @param threshold the {@link Threshold} used to transform the pairs
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link DiscreteProbabilityPairs}
     * @return the {@link DiscreteProbabilityPairs}
     */

    DiscreteProbabilityPairs transform( EnsemblePairs input,
                                        Threshold threshold,
                                        BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper );

    /**
     * Converts a {@link PairOfDoubleAndVectorOfDoubles} to a {@link PairOfDoubles} that contains the probabilities that
     * a discrete event occurs according to the left side and the right side, respectively. The event is represented by
     * a {@link Threshold}.
     * 
     * @param pair the pair to transform
     * @param threshold the threshold
     * @return the transformed pair
     */

    PairOfDoubles transform( PairOfDoubleAndVectorOfDoubles pair, Threshold threshold );

    /**
     * Returns a function that converts a {@link PairOfDoubleAndVectorOfDoubles} to a {@link PairOfDoubles} by 
     * applying the specified transformer to the {@link PairOfDoubleAndVectorOfDoubles#getItemTwo()}.
     * 
     * @param transformer the transformer
     * @return a composed function
     */

    Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> transform( ToDoubleFunction<double[]> transformer );

    /**
     * A transformer that applies a predicate to the left and each of the right separately, returning a transformed
     * pair or null if the left and none of the right meet the condition.
     * 
     * @param predicate the input predicate
     * @return a composed function
     */

    Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubleAndVectorOfDoubles>
            leftAndEachOfRight( DoublePredicate predicate );

    /**
     * Returns a function to compute a value from the sorted array that corresponds to the input non-exceedence 
     * probability. This method produces undefined results if the input array is unsorted. Corresponds to 
     * method 6 in the R function, quantile{stats}: <a href=
     * "https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html">https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html</a>.
     * Also see: <a href=
     * "https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample">https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample</a>.
     * 
     * @param sorted the sorted input array
     * @return the threshold
     */

    DoubleUnaryOperator getQuantileFunction( double[] sorted );

    /**
     * Returns a {@link Threshold} with quantiles defined from a prescribed {@link Threshold} with probabilities, 
     * where the quantiles are mapped using {@link #getQuantileFunction(double[])}.
     * 
     * @param sorted the sorted input array
     * @param threshold the probability threshold from which the quantile threshold is determined
     * @param decimals an optional number of decimal places to which the threshold will be rounded up (may be null)
     * @return the probability threshold
     */

    Threshold getQuantileFromProbability( Threshold threshold, double[] sorted, Integer decimals );

}
