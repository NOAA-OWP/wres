package wres.datamodel.metric;

import java.util.List;
import java.util.Objects;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.VectorOfBooleans;

/**
 * A default factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class DefaultMetricInputFactory implements MetricInputFactory
{

    /**
     * Instance of the factory.
     */
    
    private static MetricInputFactory instance = null;
    
    /**
     * Returns an instance of a {@link MetricOutputFactory}.
     * 
     * @return a {@link MetricOutputFactory}
     */
    
    public static MetricInputFactory of() {
        if(Objects.isNull(instance)) {
            instance = new DefaultMetricInputFactory();
        }
        return instance;
    }    
    
    /**
     * Construct the dichotomous input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */
    @Override
    public DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return (DichotomousPairs)new SafeDichotomousPairs.DichotomousPairsBuilder().setData(pairs)
                                                                                   .setMetadata(meta)
                                                                                   .build();
    }

    /**
     * Construct the multicategory input without any pairs for a baseline.
     * 
     * @param pairs the verification pairs
     * @param meta the metadata
     * @return the pairs
     * @throws MetricInputException if the inputs are invalid
     */
    @Override
    public MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return new SafeMulticategoryPairs.MulticategoryPairsBuilder().setData(pairs).setMetadata(meta).build();
    }

    /**
     * Construct the discrete probability input without any pairs for a baseline.
     * 
     * @param pairs the discrete probability pairs
     * @param meta the metadata
     * @throws MetricInputException if the inputs are invalid
     * @return the pairs
     */
    @Override
    public DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final Metadata meta)
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
    @Override
    public DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
                                                                      final List<PairOfDoubles> basePairs,
                                                                      final Metadata mainMeta,
                                                                      final Metadata baselineMeta)
    {
        final SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder b =
                                                                             new SafeDiscreteProbabilityPairs.DiscreteProbabilityPairsBuilder();
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
    @Override
    public SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Metadata meta)
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
    @Override
    public SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
                                                        final List<PairOfDoubles> basePairs,
                                                        final Metadata mainMeta,
                                                        final Metadata baselineMeta)
    {
        final SafeSingleValuedPairs.SingleValuedPairsBuilder b = new SafeSingleValuedPairs.SingleValuedPairsBuilder();
        b.setMetadata(mainMeta);
        b.setData(pairs);
        b.setDataForBaseline(basePairs);
        b.setMetadataForBaseline(baselineMeta);
        return b.build();
    }

    /**
     * Prevent construction.
     */

    private DefaultMetricInputFactory()
    {
    }

}
