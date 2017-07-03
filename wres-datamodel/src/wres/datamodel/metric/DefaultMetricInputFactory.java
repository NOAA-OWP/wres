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

public class DefaultMetricInputFactory extends DefaultMetricDataFactory implements MetricInputFactory
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

    public static MetricInputFactory getInstance()
    {
        if(Objects.isNull(instance))
        {
            instance = new DefaultMetricInputFactory();
        }
        return instance;
    }

    @Override
    public DichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return (DichotomousPairs)new SafeDichotomousPairs.DichotomousPairsBuilder().setData(pairs)
                                                                                   .setMetadata(meta)
                                                                                   .build();
    }

    @Override
    public MulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return new SafeMulticategoryPairs.MulticategoryPairsBuilder().setData(pairs).setMetadata(meta).build();
    }

    @Override
    public DiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofDiscreteProbabilityPairs(pairs, null, meta, null);
    }

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

    @Override
    public SingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofSingleValuedPairs(pairs, null, meta, null);
    }

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
