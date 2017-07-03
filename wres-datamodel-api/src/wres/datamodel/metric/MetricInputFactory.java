package wres.datamodel.metric;

import java.util.List;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;

/**
 * A factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface MetricInputFactory extends MetricDataFactory
{

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs, final Metadata meta);

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs, final Metadata meta);

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final Metadata meta);
    /**
     * Construct the discrete probability input with a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param basePairs the baseline pairs
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final List<PairOfDoubles> basePairs,
                                                                      final Metadata mainMeta,
                                                                      final Metadata baselineMeta);

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Metadata meta);

    /**
     * Construct the single-valued input with a baseline.
     * 
     * @param pairs the single-valued pairs
     * @param basePairs the baseline pairs
     * @param mainMeta the metadata for the main pairs
     * @param baselineMeta the metadata for the baseline pairs
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                        final List<PairOfDoubles> basePairs,
                                                        final Metadata mainMeta,
                                                        final Metadata baselineMeta);
}
