package wres.datamodel;

import java.util.List;
import java.util.Map;

import wres.datamodel.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.MetricOutputForProjectByLeadThreshold.MetricOutputForProjectByLeadThresholdBuilder;
import wres.datamodel.MetricOutputMultiMapByLeadThreshold.MetricOutputMultiMapByLeadThresholdBuilder;
import wres.datamodel.Threshold.Operator;

/**
 * A factory class for producing datasets associated with verification metrics.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface DataFactory
{

    /**
     * Convenience method that returns a {@link MapBiKey} to map a {@link MetricOutput} by forecast lead time and
     * {@link Threshold}.
     * 
     * @param leadTime the forecast lead time
     * @param threshold the threshold value
     * @param condition the threshold condition
     * @return a map key
     */

    default MapBiKey<Integer, Threshold> getMapKeyByLeadThreshold(final Integer leadTime,
                                                                  final Double threshold,
                                                                  final Operator condition)
    {
        return getMapKey(leadTime, getThreshold(threshold, condition));
    }

    /**
     * Convenience method that returns a {@link MapBiKey} to map a {@link MetricOutput} by forecast lead time and
     * {@link Threshold}.
     * 
     * @param leadTime the forecast lead time
     * @param threshold the threshold value or lower bound of a {@link Operator#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Operator#BETWEEN} or null
     * @param condition the threshold condition
     * @return a map key
     */

    default MapBiKey<Integer, Threshold> getMapKeyByLeadThreshold(final Integer leadTime,
                                                                  final Double threshold,
                                                                  final Double thresholdUpper,
                                                                  final Operator condition)
    {
        return getMapKey(leadTime, getThreshold(threshold, thresholdUpper, condition));
    }

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param threshold the threshold value or lower bound
     * @param condition the threshold condition
     * @return a threshold
     */

    default Threshold getThreshold(final Double threshold, final Operator condition)
    {
        return getThreshold(threshold, null, condition);
    }

    /**
     * Returns {@link ProbabilityThreshold} from the specified input. The input must be in the unit interval, [0,1].
     * 
     * @param threshold the threshold value or lower bound
     * @param condition the threshold condition
     * @return a threshold
     */

    default ProbabilityThreshold getProbabilityThreshold(final Double threshold, final Operator condition)
    {
        return getProbabilityThreshold(threshold, null, condition);
    }

    /**
     * Returns a {@link QuantileThreshold} from the specified input
     * 
     * @param threshold the threshold value
     * @param probability the probability associated with the threshold
     * @param condition the threshold condition
     * @return a quantile
     */

    default QuantileThreshold getQuantileThreshold(final Double threshold,
                                                   final Double probability,
                                                   final Operator condition)
    {
        return getQuantileThreshold(threshold, null, probability, null, condition);
    }

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return ofDichotomousPairs(pairs, null, meta, null, null);
    }

    /**
     * Construct the dichotomous input from atomic {@link PairOfBooleans} without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairsFromAtomic(final List<PairOfBooleans> pairs, final Metadata meta)
    {
        return ofDichotomousPairsFromAtomic(pairs, null, meta, null, null);
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return ofMulticategoryPairs(pairs, null, meta, null, null);
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofDiscreteProbabilityPairs(pairs, null, meta, null, null);
    }

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofSingleValuedPairs(pairs, null, meta, null, null);
    }

    /**
     * Construct the ensemble input without any pairs for a baseline.
     * 
     * @param pairs the ensemble pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default EnsemblePairs ofEnsemblePairs(final List<PairOfDoubleAndVectorOfDoubles> pairs, final Metadata meta)
    {
        return ofEnsemblePairs(pairs, null, meta, null, null);
    }

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs,
                                                final Metadata meta,
                                                final VectorOfDoubles climatology)
    {
        return ofDichotomousPairs(pairs, null, meta, null, climatology);
    }

    /**
     * Construct the dichotomous input from atomic {@link PairOfBooleans} without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairsFromAtomic(final List<PairOfBooleans> pairs,
                                                          final Metadata meta,
                                                          final VectorOfDoubles climatology)
    {
        return ofDichotomousPairsFromAtomic(pairs, null, meta, null, climatology);
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs,
                                                    final Metadata meta,
                                                    final VectorOfDoubles climatology)
    {
        return ofMulticategoryPairs(pairs, null, meta, null, climatology);
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                final Metadata meta,
                                                                final VectorOfDoubles climatology)
    {
        return ofDiscreteProbabilityPairs(pairs, null, meta, null, climatology);
    }

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                  final Metadata meta,
                                                  final VectorOfDoubles climatology)
    {
        return ofSingleValuedPairs(pairs, null, meta, null, climatology);
    }

    /**
     * Construct the ensemble input without any pairs for a baseline.
     * 
     * @param pairs the ensemble pairs
     * @param meta the metadata
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default EnsemblePairs ofEnsemblePairs(final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                          final Metadata meta,
                                          final VectorOfDoubles climatology)
    {
        return ofEnsemblePairs(pairs, null, meta, null, climatology);
    }

    /**
     * Return a {@link VectorOutput} with a default decomposition template of {@link MetricDecompositionGroup#NONE}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link VectorOutput}
     */

    default VectorOutput ofVectorOutput(final double[] output, final MetricOutputMetadata meta)
    {
        return ofVectorOutput(output, MetricDecompositionGroup.NONE, meta);
    }

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                  final List<PairOfDoubles> basePairs,
                                                  final Metadata mainMeta,
                                                  final Metadata baselineMeta)
    {
        return ofSingleValuedPairs(pairs, basePairs, mainMeta, baselineMeta, null);
    }

    /**
     * Construct the ensemble input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default EnsemblePairs ofEnsemblePairs(final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                          final List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                          final Metadata mainMeta,
                                          final Metadata baselineMeta)
    {
        return ofEnsemblePairs(pairs, basePairs, mainMeta, baselineMeta, null);
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs,
                                                    final List<VectorOfBooleans> basePairs,
                                                    final Metadata mainMeta,
                                                    final Metadata baselineMeta)
    {
        return ofMulticategoryPairs(pairs, basePairs, mainMeta, baselineMeta, null);
    }

    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    default DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                final List<PairOfDoubles> basePairs,
                                                                final Metadata mainMeta,
                                                                final Metadata baselineMeta)
    {
        return ofDiscreteProbabilityPairs(pairs, basePairs, mainMeta, baselineMeta, null);
    }

    /**
     * Construct the dichotomous input with pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    default DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs,
                                                final List<VectorOfBooleans> basePairs,
                                                final Metadata mainMeta,
                                                final Metadata baselineMeta)
    {
        return ofDichotomousPairs(pairs, basePairs, mainMeta, baselineMeta, null);
    }

    /**
     * Returns a {@link MetadataFactory} for building {@link Metadata}.
     * 
     * @return an instance of {@link MetadataFactory}
     */

    MetadataFactory getMetadataFactory();

    /**
     * Returns a {@link Slicer} for slicing data.
     * 
     * @return a {@link Slicer}
     */

    Slicer getSlicer();

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                          final List<PairOfDoubles> basePairs,
                                          final Metadata mainMeta,
                                          final Metadata baselineMeta,
                                          final VectorOfDoubles climatology);

    /**
     * Construct the ensemble input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    EnsemblePairs ofEnsemblePairs(final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                  final List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                  final Metadata mainMeta,
                                  final Metadata baselineMeta,
                                  final VectorOfDoubles climatology);

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs,
                                            final List<VectorOfBooleans> basePairs,
                                            final Metadata mainMeta,
                                            final Metadata baselineMeta,
                                            final VectorOfDoubles climatology);

    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                        final List<PairOfDoubles> basePairs,
                                                        final Metadata mainMeta,
                                                        final Metadata baselineMeta,
                                                        final VectorOfDoubles climatology);

    /**
     * Construct the dichotomous input with pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs,
                                        final List<VectorOfBooleans> basePairs,
                                        final Metadata mainMeta,
                                        final Metadata baselineMeta,
                                        final VectorOfDoubles climatology);

    /**
     * Construct the dichotomous input from atomic {@link PairOfBooleans} with pairs for a baseline.
     * 
     * @param pairs the main verification pairs
     * @param basePairs the baseline pairs (may be null)
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs (may be null, if the basePairs are null)
     * @param climatology an optional climatological dataset (may be null)
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    DichotomousPairs ofDichotomousPairsFromAtomic(final List<PairOfBooleans> pairs,
                                                  final List<PairOfBooleans> basePairs,
                                                  final Metadata mainMeta,
                                                  final Metadata baselineMeta,
                                                  final VectorOfDoubles climatology);

    /**
     * Return a {@link PairOfDoubles} from two double values.
     * 
     * @param left the left value
     * @param right the right value
     * @return the pair
     */

    PairOfDoubles pairOf(final double left, final double right);

    /**
     * Return a {@link PairOfBooleans} from two boolean values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfBooleans pairOf(final boolean left, final boolean right);

    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfDoubleAndVectorOfDoubles pairOf(final double left, final double[] right);

    /**
     * Return a {@link PairOfDoubleAndVectorOfDoubles} from a double value and a double vector of values.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    PairOfDoubleAndVectorOfDoubles pairOf(final Double left, final Double[] right);

    /**
     * Return a {@link Pair} from two double vectors.
     * 
     * @param left the first value
     * @param right the second value
     * @return the pair
     */

    Pair<VectorOfDoubles, VectorOfDoubles> pairOf(final double[] left, final double[] right);

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     * 
     * @param vec the vector of doubles
     * @return the vector
     */

    VectorOfDoubles vectorOf(final double[] vec);

    /**
     * Return a {@link VectorOfDoubles} from a vector of doubles
     * 
     * @param vec the vector of doubles
     * @return the vector
     */

    VectorOfDoubles vectorOf(final Double[] vec);

    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     * 
     * @param vec the vector of booleans
     * @return the vector
     */

    VectorOfBooleans vectorOf(final boolean[] vec);

    /**
     * Return a {@link VectorOfBooleans} from a vector of booleans
     * 
     * @param vec the vector of booleans
     * @return the vector
     */

    MatrixOfDoubles matrixOf(final double[][] vec);

    /**
     * Return a {@link ScalarOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link ScalarOutput}
     */

    ScalarOutput ofScalarOutput(final double output, final MetricOutputMetadata meta);

    /**
     * Return a {@link VectorOutput} with a prescribed decomposition template {@link MetricDecompositionGroup}, which
     * maps the output to specific components in a specific order.
     * 
     * @param output the output data
     * @param template the template for the output
     * @param meta the metadata
     * @return a {@link VectorOutput}
     */

    VectorOutput ofVectorOutput(final double[] output,
                                final MetricDecompositionGroup template,
                                final MetricOutputMetadata meta);

    /**
     * Return a {@link MultiVectorOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MultiVectorOutput}
     */

    MultiVectorOutput ofMultiVectorOutput(final Map<MetricConstants, double[]> output, final MetricOutputMetadata meta);

    /**
     * Return a {@link MatrixOutput}.
     * 
     * @param output the output data
     * @param meta the metadata
     * @return a {@link MatrixOutput}
     */

    MatrixOutput ofMatrixOutput(final double[][] output, final MetricOutputMetadata meta);

    /**
     * Returns a {@link MapKey} to map a {@link MetricOutput} by an elementary key.
     * 
     * @param <S> the type of key
     * @param key the key
     * @return a map key
     */

    <S extends Comparable<S>> MapKey<S> getMapKey(S key);

    /**
     * Returns a {@link MapBiKey} to map a {@link MetricOutput} by two elementary keys.
     * 
     * @param <S> the type of the first key
     * @param <T> the type of the second key
     * @param firstKey the first key
     * @param secondKey the second key
     * @return a map key
     */

    <S extends Comparable<S>, T extends Comparable<T>> MapBiKey<S, T> getMapKey(S firstKey, T secondKey);

    /**
     * Returns {@link Threshold} from the specified input.
     * 
     * @param threshold the threshold value or lower bound of a {@link Operator#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Operator#BETWEEN} or null
     * @param condition the threshold condition
     * @return a threshold
     */

    Threshold getThreshold(final Double threshold, final Double thresholdUpper, final Operator condition);

    /**
     * Returns {@link ProbabilityThreshold} from the specified input. Both inputs must be in the unit interval, [0,1].
     * 
     * @param threshold the threshold value or lower bound of a {@link Operator#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Operator#BETWEEN} or null
     * @param condition the threshold condition
     * @return a threshold
     */

    ProbabilityThreshold getProbabilityThreshold(final Double threshold,
                                                 final Double thresholdUpper,
                                                 final Operator condition);

    /**
     * Returns a {@link QuantileThreshold} from the specified input
     * 
     * @param threshold the threshold value or lower bound of a {@link Operator#BETWEEN} condition
     * @param thresholdUpper the upper threshold of a {@link Operator#BETWEEN} or null
     * @param probability the probability associated with the threshold
     * @param probabilityUpper the probability associated with the upper threshold or null
     * @param condition the threshold condition
     * @return a quantile
     */

    QuantileThreshold getQuantileThreshold(final Double threshold,
                                           final Double thresholdUpper,
                                           Double probability,
                                           Double probabilityUpper,
                                           final Operator condition);

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} from the raw map of inputs.
     * 
     * @param <T> the type of output
     * @param input the map of metric outputs
     * @return a {@link MetricOutputMapByLeadThreshold} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> ofMap(final Map<MapBiKey<Integer, Threshold>, T> input);

    /**
     * Returns a {@link MetricOutputMultiMapByLeadThreshold} from a map of inputs by lead time and {@link Threshold}.
     * 
     * @param <T> the type of output
     * @param input the input map of metric outputs by lead time and threshold
     * @return a map of metric outputs by lead time and threshold for several metrics
     */

    <T extends MetricOutput<?>> MetricOutputMultiMapByLeadThreshold<T> ofMultiMap(final Map<MapBiKey<Integer, Threshold>, MetricOutputMapByMetric<T>> input);

    /**
     * Returns a builder for a {@link MetricOutputMultiMapByLeadThreshold} that allows for the incremental addition of
     * {@link MetricOutputMapByLeadThreshold} as they are computed.
     * 
     * @param <T> the type of output
     * @return a {@link MetricOutputMultiMapByLeadThresholdBuilder} for a map of metric outputs by lead time and
     *         threshold
     */

    <T extends MetricOutput<?>> MetricOutputMultiMapByLeadThresholdBuilder<T> ofMultiMap();

    /**
     * Returns a builder for a {@link MetricOutputForProjectByLeadThreshold}.
     * 
     * @return a {@link MetricOutputForProjectByLeadThresholdBuilder} for a map of metric outputs by lead time and
     *         threshold
     */

    MetricOutputForProjectByLeadThresholdBuilder ofMetricOutputForProjectByLeadThreshold();

    /**
     * Returns a {@link MetricOutputMapByMetric} from the raw list of inputs.
     * 
     * @param <T> the type of output
     * @param input the list of metric outputs
     * @return a {@link MetricOutputMapByMetric} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByMetric<T> ofMap(final List<T> input);

    /**
     * Combines a list of {@link MetricOutputMapByLeadThreshold} into a single map.
     * 
     * @param <T> the type of output
     * @param input the list of input maps
     * @return a combined {@link MetricOutputMapByLeadThreshold} of metric outputs
     */

    <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> combine(final List<MetricOutputMapByLeadThreshold<T>> input);

    /**
     * Helper that checks for the equality of two double values using a prescribed number of significant digits.
     * 
     * @param first the first double
     * @param second the second double
     * @param digits the number of significant digits
     * @return true if the first and second are equal to the number of significant digits
     */

    boolean doubleEquals(double first, double second, int digits);

}
