package wres.engine.statistics.metric.inputs;

import java.util.List;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.metric.Metadata;

/**
 * A factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricInputFactory
{

    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return (DichotomousPairs)new DichotomousPairs.DichotomousPairsBuilder().setData(pairs).setMetadata(meta).build();
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return new MulticategoryPairs.MulticategoryPairsBuilder().setData(pairs).setMetadata(meta).build();
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofDiscreteProbabilityPairs(pairs, null, meta, null);
    }

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

    public static DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final List<PairOfDoubles> basePairs,
                                                                      final Metadata mainMeta,
                                                                      final Metadata baselineMeta)
    {
        final DiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder b =
                                                                         new DiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder();
        b.setData(pairs);
        b.setMetadata(mainMeta);
        b.setDataForBaseline(basePairs);
        b.setMetadataForBaseline(baselineMeta);
        return b.build();
    }

    /**
     * Construct the single-valued input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */

    public static SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofSingleValuedPairs(pairs, null, meta, null);
    }

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

    public static SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                        final List<PairOfDoubles> basePairs,
                                                        final Metadata mainMeta,
                                                        final Metadata baselineMeta)
    {
        final SingleValuedPairs.SingleValuedPairsBuilder b = new SingleValuedPairs.SingleValuedPairsBuilder();
        b.setMetadata(mainMeta);
        b.setData(pairs);
        b.setDataForBaseline(basePairs);
        b.setMetadataForBaseline(baselineMeta);
        return b.build();
    }

    /**
     * Return a cast of the inputs pairs.
     * 
     * @param pairs the input pairs
     * @param meta the dimension
     * @param <T> the output pairs
     * @return the casted pairs
     */

    @SuppressWarnings("unchecked")
    public static <T extends SingleValuedPairs> T ofExtendsSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                                             final Metadata meta)
    {
        return (T)ofSingleValuedPairs(pairs, null, meta, null);
    }

    /**
     * Prevent construction.
     */

    private MetricInputFactory()
    {

    }

}
