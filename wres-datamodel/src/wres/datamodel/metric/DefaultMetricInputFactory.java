package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import wres.datamodel.Pair;
import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.SafeMatrixOfDoubles;
import wres.datamodel.SafePairOfDoubleAndVectorOfDoubles;
import wres.datamodel.SafePairOfDoubles;
import wres.datamodel.SafeVectorOfBooleans;
import wres.datamodel.SafeVectorOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.VectorOfDoubles;

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
    public SafeDichotomousPairs ofDichotomousPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return (SafeDichotomousPairs)new SafeDichotomousPairs.DichotomousPairsBuilder().setData(pairs)
                                                                                       .setMetadata(meta)
                                                                                       .build();
    }

    @Override
    public SafeMulticategoryPairs ofMulticategoryPairs(final List<VectorOfBooleans> pairs, final Metadata meta)
    {
        return new SafeMulticategoryPairs.MulticategoryPairsBuilder().setData(pairs).setMetadata(meta).build();
    }

    @Override
    public SafeDiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofDiscreteProbabilityPairs(pairs, null, meta, null);
    }

    @Override
    public SafeDiscreteProbabilityPairs ofDiscreteProbabilityPairs(final List<PairOfDoubles> pairs,
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
    public SafeSingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs, final Metadata meta)
    {
        return ofSingleValuedPairs(pairs, null, meta, null);
    }

    @Override
    public SafeSingleValuedPairs ofSingleValuedPairs(final List<PairOfDoubles> pairs,
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
    public SafePairOfDoubles pairOf(final double left, final double right)
    {
        return SafePairOfDoubles.of(left, right);
    }

    @Override
    public SafePairOfBooleans pairOf(final boolean left, final boolean right)
    {
        return new SafePairOfBooleans(left, right);
    }

    @Override
    public SafePairOfDoubleAndVectorOfDoubles pairOf(final double left, final double[] right)
    {
        return (SafePairOfDoubleAndVectorOfDoubles)SafePairOfDoubleAndVectorOfDoubles.of(left, right);
    }

    @Override
    public SafePairOfDoubleAndVectorOfDoubles pairOf(final Double left, final Double[] right)
    {
        return (SafePairOfDoubleAndVectorOfDoubles)SafePairOfDoubleAndVectorOfDoubles.of(left, right);
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
    public SafeVectorOfDoubles vectorOf(final double[] vec)
    {
        return (SafeVectorOfDoubles)SafeVectorOfDoubles.of(vec);
    }

    @Override
    public SafeVectorOfDoubles vectorOf(final Double[] vec)
    {
        return (SafeVectorOfDoubles)SafeVectorOfDoubles.of(vec);
    }

    @Override
    public SafeVectorOfBooleans vectorOf(final boolean[] vec)
    {
        return (SafeVectorOfBooleans)SafeVectorOfBooleans.of(vec);
    }

    @Override
    public SafeMatrixOfDoubles matrixOf(final double[][] vec)
    {
        return (SafeMatrixOfDoubles)SafeMatrixOfDoubles.of(vec);
    }

    /**
     * Returns a safe type from the input by either casting the elementary pairs or creating new elementary pairs that
     * are safe. Returns the output in an immutable list.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    List<PairOfDoubles> getSafePairOfDoublesList(List<PairOfDoubles> input)
    {
        Objects.requireNonNull(input, "Specify a non-null input from which to create a safe type.");
        List<PairOfDoubles> returnMe = new ArrayList<>();
        input.forEach(value -> {
            if(value instanceof SafePairOfDoubles)
            {
                returnMe.add(value);
            }
            else
            {
                returnMe.add(pairOf(value.getItemOne(), value.getItemTwo()));
            }
        });
        return Collections.unmodifiableList(returnMe);
    }
    
    /**
     * Returns a safe type from the input by either casting the elementary pairs or creating new elementary pairs that
     * are safe. Returns the output in an immutable list.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    List<VectorOfBooleans> getSafeVectorOfBooleansList(List<VectorOfBooleans> input)
    {
        Objects.requireNonNull(input, "Specify a non-null input from which to create a safe type.");
        List<VectorOfBooleans> returnMe = new ArrayList<>();
        input.forEach(value -> {
            if(value instanceof SafeVectorOfBooleans)
            {
                returnMe.add(value);
            }
            else
            {
                returnMe.add(vectorOf(value.getBooleans()));
            }
        });
        return Collections.unmodifiableList(returnMe);
    }    

    /**
     * Default implementation of a pair of booleans.
     */

    class SafePairOfBooleans implements PairOfBooleans
    {
        private final boolean left;
        private final boolean right;

        /**
         * Construct.
         * 
         * @param left the left
         * @param right the right
         */

        private SafePairOfBooleans(boolean left, boolean right)
        {
            this.left = left;
            this.right = right;
        }

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

    /**
     * Prevent construction.
     */

    private DefaultMetricInputFactory()
    {
    }

}
