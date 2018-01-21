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

import wres.datamodel.inputs.MetricInputSliceException;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.PairOfBooleans;
import wres.datamodel.inputs.pairs.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.inputs.pairs.PairOfDoubles;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MultiValuedScoreOutput;

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
     * Returns a {@link Threshold} with quantiles defined from the prescribed {@link Threshold} with probabilities, 
     * where the quantiles are mapped using {@link #getQuantileFunction(double[])}.
     * 
     * @param sorted the sorted input array
     * @param threshold the probability threshold from which the quantile is determined
     * @return the quantile threshold
     */

    default Threshold getQuantileFromProbability( Threshold threshold, double[] sorted )
    {
        return getQuantileFromProbability( threshold, sorted, null );
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
     * Returns the subset of pairs where the left and right both meet the prescribed condition. Applies to both the 
     * main pairs and any baseline, by default, and optionally to any climatological data associated with the pairs.
     * 
     * @param input the {@link SingleValuedPairs} to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology is true to apply the filter to the climatology also, false otherwise
     * @return the subset of pairs that meet the condition
     * @throws MetricInputSliceException if the slice contains no elements
     */

    SingleValuedPairs filter( SingleValuedPairs input, DoublePredicate condition, boolean applyToClimatology )
            throws MetricInputSliceException;

    /**
     * Returns the subset of pairs where the left meets the prescribed condition and one or more elements of the right
     * meet the prescribed condition, returning only those elements of the right that meet the prescribed condition. 
     * Applies to both the main pairs and any baseline, by default, and optionally to any climatological data 
     * associated with the pairs.
     * 
     * @param input the {@link EnsemblePairs} to slice
     * @param condition the condition on which to slice
     * @param applyToClimatology is true to apply the filter to the climatology also, false otherwise
     * @return the subset of pairs that meet the condition
     * @throws MetricInputSliceException if the slice contains no elements
     */

    EnsemblePairs filter( EnsemblePairs input, DoublePredicate condition, boolean applyToClimatology )
            throws MetricInputSliceException;

    /**
     * Returns a subset of pairs where the {@link Threshold} is met on the left side or null for the empty subset.
     * 
     * @param input the {@link SingleValuedPairs} to slice
     * @param threshold the {@link Threshold} on which to slice
     * @return the subset of pairs that meet the condition
     * @throws MetricInputSliceException if the slice contains no elements
     */

    SingleValuedPairs filterByLeft( SingleValuedPairs input, Threshold threshold ) throws MetricInputSliceException;

    /**
     * Returns a subset of pairs where the {@link Threshold} is met on the left side or null for the empty subset.
     * 
     * @param input the {@link EnsemblePairs} to slice
     * @param threshold the {@link Threshold} on which to slice
     * @return the subset of pairs that meet the condition
     * @throws MetricInputSliceException if the slice contains no elements
     */

    EnsemblePairs filterByLeft( EnsemblePairs input, Threshold threshold ) throws MetricInputSliceException;

    /**
     * Returns as many lists of {@link PairOfDoubleAndVectorOfDoubles} as groups of atomic pairs in the input with an
     * equal number of elements. i.e. each list of {@link PairOfDoubleAndVectorOfDoubles} in the returned result has an
     * equal number of elements, internally, and a different number of elements than all other subsets of pairs. The
     * subsets are returned in a map, indexed by the number of elements on the right side.
     * 
     * @param input a list of {@link PairOfDoubleAndVectorOfDoubles} to slice
     * @return as many subsets of {@link PairOfDoubleAndVectorOfDoubles} as groups of pairs in the input of equal size
     */

    Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> filterByRight( List<PairOfDoubleAndVectorOfDoubles> input );

    /**
     * Returns a map of {@link DoubleScoreOutput} for each component in the input map of {@link MultiValuedScoreOutput}. 
     * The slices are mapped to their {@link MetricConstants} component identifier.
     * 
     * @param input the input map
     * @return the input map sliced by component identifier
     */

    Map<MetricConstants, MetricOutputMapByTimeAndThreshold<DoubleScoreOutput>>
            filterByMetricComponent( MetricOutputMapByTimeAndThreshold<MultiValuedScoreOutput> input );

    /**
     * Produces a {@link List} of {@link PairOfDoubles} from a {@link List} of {@link PairOfDoubleAndVectorOfDoubles}
     * using a prescribed mapper function.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link SingleValuedPairs}
     * @return the {@link SingleValuedPairs}
     */

    List<PairOfDoubles> transformPairs( List<PairOfDoubleAndVectorOfDoubles> input,
                                        Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper );

    /**
     * Produces {@link DichotomousPairs} from a {@link SingleValuedPairs} by applying a mapper function to the input.
     * 
     * @param input the {@link SingleValuedPairs} pairs
     * @param mapper the function that maps from {@link SingleValuedPairs} to {@link DichotomousPairs}
     * @return the {@link DichotomousPairs}
     */

    DichotomousPairs transformPairs( SingleValuedPairs input, Function<PairOfDoubles, PairOfBooleans> mapper );

    /**
     * Produces {@link SingleValuedPairs} from a {@link EnsemblePairs} by applying a mapper function to the input.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link SingleValuedPairs} pairs
     * @return the {@link SingleValuedPairs}
     */

    SingleValuedPairs transformPairs( EnsemblePairs input,
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

    DiscreteProbabilityPairs transformPairs( EnsemblePairs input,
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

    PairOfDoubles transformPair( PairOfDoubleAndVectorOfDoubles pair, Threshold threshold );

    /**
     * Converts a {@link PairOfDoubleAndVectorOfDoubles} to a {@link PairOfDoubles} by retrieving the first element of
     * the right hand side from the paired {@link VectorOfDoubles}.
     * 
     * @param pair the pair to transform
     * @return the transformed pair
     */

    PairOfDoubles transformPair( PairOfDoubleAndVectorOfDoubles pair );

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
