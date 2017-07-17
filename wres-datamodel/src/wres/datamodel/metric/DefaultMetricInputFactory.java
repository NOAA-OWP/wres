package wres.datamodel.metric;

import java.util.List;
import java.util.Objects;

import wres.datamodel.*;

/**
 * A default factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
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
    
    @Override
    public PairOfDoubles pairOf(final double left, final double right)
    {
        return DataFactory.pairOf(left, right);
    }
    
    @Override
    public PairOfBooleans pairOf(final boolean left, final boolean right)
    {
        return new PairOfBooleans()
        {
            @Override
            public boolean getItemOne()
            {
                return left;
            }

            @Override
            public boolean getItemTwo()
            {
                return right;
            }
        };
    }
    
    @Override
    public PairOfDoubleAndVectorOfDoubles pairOf(final double left, final double[] right)
    {
        return SafePairOfDoubleAndVectorOfDoubles.of(left, right);
    }
    
    @Override
    public PairOfDoubleAndVectorOfDoubles pairOf(final Double left, final Double[] right)
    {
        return SafePairOfDoubleAndVectorOfDoubles.of(left, right);
    }
    
    @Override
    public Pair<VectorOfDoubles, VectorOfDoubles> pairOf(final double[] left, final double[] right)
    {
        return new Pair<VectorOfDoubles, VectorOfDoubles>()
        {
            @Override
            public VectorOfDoubles getItemOne()
            {
                return SafeVectorOfDoubles.of(left);
            }

            @Override
            public VectorOfDoubles getItemTwo()
            {
                return SafeVectorOfDoubles.of(right);
            }
        };
    }
    
    @Override
    public VectorOfDoubles vectorOf(final double[] vec)
    {
        return SafeVectorOfDoubles.of(vec);
    }
    
    @Override
    public VectorOfDoubles vectorOf(final Double[] vec)
    {
        return SafeVectorOfDoubles.of(vec);
    }
    
    @Override
    public VectorOfBooleans vectorOf(final boolean[] vec)
    {
        return SafeVectorOfBooleans.of(vec);
    }
    
    @Override
    public MatrixOfDoubles matrixOf(final double[][] vec)
    {
        return SafeMatrixOfDoubles.of(vec);
    }    

    /**
     * Prevent construction.
     */

    private DefaultMetricInputFactory()
    {
    }

}
