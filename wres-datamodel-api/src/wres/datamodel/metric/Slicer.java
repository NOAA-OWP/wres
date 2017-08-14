package wres.datamodel.metric;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfDoubles;

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
     * Returns the left side of {@link SingleValuedPairs#getData()} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the left side
     */

    double[] getLeftSide(SingleValuedPairs input);

    /**
     * Returns the right side of {@link SingleValuedPairs#getData()} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the right side
     */

    double[] getRightSide(SingleValuedPairs input);

    /**
     * Returns the left side of {@link EnsemblePairs#getData()} as a primitive array of doubles.
     * 
     * @param input the input pairs
     * @return the left side
     */

    double[] getLeftSide(EnsemblePairs input);

    /**
     * Returns a subset of pairs where the {@link Threshold} is met on the left side or null for the empty subset.
     * 
     * @param input the {@link SingleValuedPairs} to slice
     * @param threshold the {@link Threshold} on which to slice
     * @return the subset of pairs that meet the condition or null
     */

    SingleValuedPairs sliceByLeft(SingleValuedPairs input, Threshold threshold);

    /**
     * Returns a subset of pairs where the {@link Threshold} is met on the left side or null for the empty subset.
     * 
     * @param input the {@link EnsemblePairs} to slice
     * @param threshold the {@link Threshold} on which to slice
     * @return the subset of pairs that meet the condition or null
     */

    EnsemblePairs sliceByLeft(EnsemblePairs input, Threshold threshold);

    /**
     * Returns as many lists of {@link PairOfDoubleAndVectorOfDoubles} as groups of atomic pairs in the input with an
     * equal number of elements. i.e. each list of {@link PairOfDoubleAndVectorOfDoubles} in the returned result has an
     * equal number of elements, internally, and a different number of elements than all other subsets of pairs. The
     * subsets are returned in a map, indexed by the number of elements on the right side.
     * 
     * @param input a list of {@link PairOfDoubleAndVectorOfDoubles} to slice
     * @return as many subsets of {@link PairOfDoubleAndVectorOfDoubles} as groups of pairs in the input of equal size
     */

    Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> sliceByRight(List<PairOfDoubleAndVectorOfDoubles> input);

    /**
     * Returns a map of {@link ScalarOutput} for each component in the input map of {@link VectorOutput}. The slices are
     * mapped to their {@link MetricConstants} component identifier.
     * 
     * @param input the input map
     * @return the input map sliced by component identifier
     */

    Map<MetricConstants, MetricOutputMapByLeadThreshold<ScalarOutput>> sliceByMetricComponent(MetricOutputMapByLeadThreshold<VectorOutput> input);

    /**
     * Produces a {@link List} of {@link PairOfDoubles} from a {@link List} of {@link PairOfDoubleAndVectorOfDoubles}
     * using a prescribed mapper function.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link SingleValuedPairs}
     * @return the {@link SingleValuedPairs}
     */

    List<PairOfDoubles> transformPairs(List<PairOfDoubleAndVectorOfDoubles> input,
                                       Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper);

    /**
     * Produces {@link DichotomousPairs} from a {@link SingleValuedPairs} by applying a mapper function to the input.
     * 
     * @param input the {@link SingleValuedPairs} pairs
     * @param mapper the function that maps from {@link SingleValuedPairs} to {@link DichotomousPairs}
     * @return the {@link DichotomousPairs}
     */

    DichotomousPairs transformPairs(SingleValuedPairs input, Function<PairOfDoubles, PairOfBooleans> mapper);

    /**
     * Produces {@link SingleValuedPairs} from a {@link EnsemblePairs} by applying a mapper function to the input.
     * 
     * @param input the {@link EnsemblePairs}
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link SingleValuedPairs} pairs
     * @return the {@link SingleValuedPairs}
     */

    SingleValuedPairs transformPairs(EnsemblePairs input,
                                     Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper);

    /**
     * Produces {@link DiscreteProbabilityPairs} from a {@link EnsemblePairs} by applying a mapper function to the input
     * using a prescribed {@link Threshold}.
     * 
     * @param input the {@link EnsemblePairs}
     * @param threshold the {@link Threshold} used to transform the pairs
     * @param mapper the function that maps from {@link EnsemblePairs} to {@link DiscreteProbabilityPairs}
     * @return the {@link DiscreteProbabilityPairs}
     */

    DiscreteProbabilityPairs transformPairs(EnsemblePairs input,
                                            Threshold threshold,
                                            BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper);

    /**
     * Converts a {@link PairOfDoubleAndVectorOfDoubles} to a {@link PairOfDoubles} that contains the probabilities that
     * a discrete event occurs according to the left side and the right side, respectively. The event is represented by
     * a {@link Threshold}.
     * 
     * @param pair the pair to transform
     * @param threshold the threshold
     * @return the transformed pair
     */

    PairOfDoubles transformPair(PairOfDoubleAndVectorOfDoubles pair, Threshold threshold);

    /**
     * Converts a {@link PairOfDoubleAndVectorOfDoubles} to a {@link PairOfDoubles} by retrieving the first element of
     * the right hand side from the paired {@link VectorOfDoubles}.
     * 
     * @param pair the pair to transform
     * @return the transformed pair
     */

    PairOfDoubles transformPair(PairOfDoubleAndVectorOfDoubles pair);

    /**
     * Returns a value from the sorted array that corresponds to the input non-exceedence probability (p). This method
     * produces undefined results if the input array is unsorted. Corresponds to method 4 in the R function,
     * quantile{stats}: <a href=
     * "https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html">https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html</a>.
     * Specifically, conducts linear interpolation of the empirical distribution function. When
     * <code>p &lt; 1 / N</code>, the first sample value is returned. When <code>p = 1</code>, the last sample value is
     * returned. Also see: <a href=
     * "https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample">https://en.wikipedia.org/wiki/Quantile#Estimating_quantiles_from_a_sample</a>.
     * 
     * @param probability the non-exceedence probability in [0,1]
     * @param sorted the sorted input array
     * @return the threshold
     */

    double getQuantile(double probability, double[] sorted);

    /**
     * Returns a {@link QuantileThreshold} for the prescribed {@link ProbabilityThreshold}, where the quantiles are
     * mapped using {@link #getQuantile(double, double[])}.
     * 
     * @param sorted the sorted input array
     * @param threshold the {@link ProbabilityThreshold} from which the {@link QuantileThreshold} is determined
     * @return the {@link QuantileThreshold}
     */

    QuantileThreshold getQuantileFromProbability(ProbabilityThreshold threshold, double[] sorted);

}
