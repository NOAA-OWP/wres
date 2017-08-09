package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import wres.datamodel.MatrixOfDoubles;
import wres.datamodel.Pair;
import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.SafeMatrixOfDoubles;
import wres.datamodel.SafePairOfDoubleAndVectorOfDoubles;
import wres.datamodel.SafePairOfDoubles;
import wres.datamodel.SafeVectorOfBooleans;
import wres.datamodel.SafeVectorOfDoubles;
import wres.datamodel.VectorOfBooleans;
import wres.datamodel.VectorOfDoubles;
import wres.datamodel.metric.SafeMetricOutputMultiMapByLeadThreshold.MetricOutputMultiMapByLeadThresholdBuilder;
import wres.datamodel.metric.Threshold.Operator;

/**
 * A default factory class for producing metric inputs.
 * 
 * @author james.brown@hydrosolved.com
 * @author jesse
 * @version 0.1
 * @since 0.1
 */

public class DefaultDataFactory implements DataFactory
{

    /**
     * Instance of the factory.
     */

    private static DataFactory instance = null;

    /**
     * Returns an instance of a {@link DataFactory}.
     * 
     * @return a {@link DataFactory}
     */

    public static DataFactory getInstance()
    {
        if(Objects.isNull(instance))
        {
            instance = new DefaultDataFactory();
        }
        return instance;
    }

    @Override
    public MetadataFactory getMetadataFactory()
    {
        return DefaultMetadataFactory.getInstance();
    }

    @Override
    public Slicer getSlicer()
    {
        return DefaultSlicer.getInstance();
    }

    @Override
    public DichotomousPairs ofDichotomousPairs(List<VectorOfBooleans> pairs,
                                               List<VectorOfBooleans> basePairs,
                                               Metadata mainMeta,
                                               Metadata baselineMeta)
    {
        final SafeDichotomousPairs.DichotomousPairsBuilder b = new SafeDichotomousPairs.DichotomousPairsBuilder();
        b.setData(pairs);
        b.setMetadata(mainMeta);
        b.setDataForBaseline(basePairs);
        b.setMetadataForBaseline(baselineMeta);
        return b.build();
    }

    @Override
    public DichotomousPairs ofDichotomousPairsFromAtomic(List<PairOfBooleans> pairs,
                                                         List<PairOfBooleans> basePairs,
                                                         Metadata mainMeta,
                                                         Metadata baselineMeta)
    {
        final SafeDichotomousPairs.DichotomousPairsBuilder b = new SafeDichotomousPairs.DichotomousPairsBuilder();
        b.setDataFromAtomic(pairs);
        b.setMetadata(mainMeta);
        b.setDataForBaselineFromAtomic(basePairs);
        b.setMetadataForBaseline(baselineMeta);
        return b.build();
    }

    @Override
    public MulticategoryPairs ofMulticategoryPairs(List<VectorOfBooleans> pairs,
                                                   List<VectorOfBooleans> basePairs,
                                                   Metadata mainMeta,
                                                   Metadata baselineMeta)
    {
        final SafeMulticategoryPairs.MulticategoryPairsBuilder b =
                                                                 new SafeMulticategoryPairs.MulticategoryPairsBuilder();
        b.setData(pairs);
        b.setMetadata(mainMeta);
        b.setDataForBaseline(basePairs);
        b.setMetadataForBaseline(baselineMeta);
        return b.build();
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
    public EnsemblePairs ofEnsemblePairs(final List<PairOfDoubleAndVectorOfDoubles> pairs,
                                         final List<PairOfDoubleAndVectorOfDoubles> basePairs,
                                         final Metadata mainMeta,
                                         final Metadata baselineMeta)
    {
        final SafeEnsemblePairs.EnsemblePairsBuilder b = new SafeEnsemblePairs.EnsemblePairsBuilder();
        b.setMetadata(mainMeta);
        b.setData(pairs);
        b.setDataForBaseline(basePairs);
        b.setMetadataForBaseline(baselineMeta);
        return b.build();
    }

    @Override
    public PairOfDoubles pairOf(final double left, final double right)
    {
        return SafePairOfDoubles.of(left, right);
    }

    @Override
    public PairOfBooleans pairOf(final boolean left, final boolean right)
    {
        return new SafePairOfBooleans(left, right);
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

    @Override
    public ScalarOutput ofScalarOutput(final double output, final MetricOutputMetadata meta)
    {
        return new SafeScalarOutput(output, meta);
    }

    @Override
    public VectorOutput ofVectorOutput(final double[] output, final MetricOutputMetadata meta)
    {
        return new SafeVectorOutput(vectorOf(output), meta);
    }

    @Override
    public MultiVectorOutput ofMultiVectorOutput(final Map<MetricConstants, double[]> output,
                                                 final MetricOutputMetadata meta)
    {
        EnumMap<MetricConstants, VectorOfDoubles> map = new EnumMap<>(MetricConstants.class);
        output.forEach((key, value) -> map.put(key, vectorOf(value)));
        return new SafeMultiVectorOutput(map, meta);
    }

    @Override
    public MatrixOutput ofMatrixOutput(final double[][] output, final MetricOutputMetadata meta)
    {
        return new SafeMatrixOutput(matrixOf(output), meta);
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByMetric<T> ofMap(final List<T> input)
    {
        Objects.requireNonNull(input, "Specify a non-null list of inputs.");
        final SafeMetricOutputMapByMetric.Builder<T> builder = new SafeMetricOutputMapByMetric.Builder<>();
        input.forEach(a -> {
            final MapBiKey<MetricConstants, MetricConstants> key = getMapKey(a.getMetadata().getMetricID(),
                                                                             a.getMetadata().getMetricComponentID());
            builder.put(key, a);
        });
        return builder.build();
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> ofMap(final Map<MapBiKey<Integer, Threshold>, T> input)
    {
        Objects.requireNonNull(input, "Specify a non-null map of inputs by lead time and threshold.");
        final SafeMetricOutputMapByLeadThreshold.Builder<T> builder =
                                                                    new SafeMetricOutputMapByLeadThreshold.Builder<>();
        input.forEach(builder::put);
        return builder.build();
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMultiMapByLeadThreshold<T> ofMultiMap(final Map<MapBiKey<Integer, Threshold>, MetricOutputMapByMetric<T>> input)
    {
        Objects.requireNonNull(input, "Specify a non-null map of inputs by threshold.");
        final MetricOutputMultiMapByLeadThresholdBuilder<T> builder =
                                                                    new MetricOutputMultiMapByLeadThresholdBuilder<>();
        input.forEach(builder::put);
        return builder.build();
    }

    @Override
    public <S extends MetricOutput<?>> MetricOutputMultiMapByLeadThreshold.Builder<S> ofMultiMap()
    {
        return new SafeMetricOutputMultiMapByLeadThreshold.MetricOutputMultiMapByLeadThresholdBuilder<>();
    }

    @Override
    public <T extends MetricOutput<?>> MetricOutputMapByLeadThreshold<T> combine(final List<MetricOutputMapByLeadThreshold<T>> input)
    {
        Objects.requireNonNull(input, "Specify a non-null map of inputs to combine.");
        final SafeMetricOutputMapByLeadThreshold.Builder<T> builder =
                                                                    new SafeMetricOutputMapByLeadThreshold.Builder<>();
        input.forEach(a -> a.forEach(builder::put));
        builder.setOverrideMetadata(input.get(0).getMetadata());
        return builder.build();
    }

    @Override
    public <S extends Comparable<S>> MapKey<S> getMapKey(final S key)
    {

        //Bounds checks
        Objects.requireNonNull(key, "Specify a non-null key.");

        /**
         * Default implementation of a {@link MapKey}.
         */

        class DefaultMapKey implements MapKey<S>
        {

            @Override
            public int compareTo(final MapKey<S> o)
            {
                //Compare the key
                Objects.requireNonNull(o, "Specify a non-null map key for comparison.");
                return key.compareTo(o.getKey());
            }

            @Override
            public S getKey()
            {
                return key;
            }

        }
        return new DefaultMapKey();
    }

    @Override
    public <S extends Comparable<S>, T extends Comparable<T>> MapBiKey<S, T> getMapKey(final S firstKey,
                                                                                       final T secondKey)
    {

        //Bounds checks
        Objects.requireNonNull(firstKey, "Specify a non-null first key.");
        Objects.requireNonNull(secondKey, "Specify a non-null second key.");

        /**
         * Default implementation of a {@link MapBiKey}.
         */

        class DefaultMapBiKey implements MapBiKey<S, T>
        {

            @Override
            public int compareTo(final MapBiKey<S, T> o)
            {
                //Compare the keys
                Objects.requireNonNull(o, "Specify a non-null map key for comparison.");
                final int returnMe = getFirstKey().compareTo(o.getFirstKey());
                if(returnMe != 0)
                {
                    return returnMe;
                }
                return getSecondKey().compareTo(o.getSecondKey());
            }

            @Override
            public S getFirstKey()
            {
                return firstKey;
            }

            @Override
            public T getSecondKey()
            {
                return secondKey;
            }
            
            @Override
            public String toString() {
                return "["+getFirstKey().toString()+", "+getSecondKey().toString()+"]";
            }
            
        }
        return new DefaultMapBiKey();
    }

    @Override
    public Threshold getThreshold(final Double threshold, final Double thresholdUpper, final Operator condition)
    {
        return new SafeThreshold(threshold, thresholdUpper, condition);
    }

    @Override
    public ProbabilityThreshold getProbabilityThreshold(final Double threshold,
                                                        final Double thresholdUpper,
                                                        final Operator condition)
    {
        return new SafeProbabilityThreshold(threshold, thresholdUpper, condition);
    }

    @Override
    public QuantileThreshold getQuantileThreshold(final Double threshold,
                                                  final Double thresholdUpper,
                                                  final Double probability,
                                                  final Double probabilityUpper,
                                                  final Operator condition)
    {
        return new SafeQuantileThreshold(threshold, thresholdUpper, probability, probabilityUpper, condition);
    }

    @Override
    public MetricOutputForProjectByLeadThreshold.Builder ofMetricOutputForProjectByThreshold()
    {
        return new SafeMetricOutputForProjectByLeadThreshold.MetricOutputForProjectByLeadThresholdBuilder();
    }

    @Override
    public boolean doubleEquals(double first, double second, int digits)
    {
        return Math.abs(first - second) < 1.0 / digits;
    }

    /**
     * Returns an immutable list that contains a safe type of the input.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    List<PairOfDoubles> safePairOfDoublesList(List<PairOfDoubles> input)
    {
        Objects.requireNonNull(input,
                               "Specify a non-null list of single-valued pairs from which to create a safe type.");
        List<PairOfDoubles> returnMe = new ArrayList<>();
        input.forEach(value -> {
            if(value instanceof SafePairOfDoubles)
            {
                returnMe.add(value);
            }
            else
            {
                returnMe.add(SafePairOfDoubles.of(value.getItemOne(), value.getItemTwo()));
            }
        });
        return Collections.unmodifiableList(returnMe);
    }

    /**
     * Returns an immutable list that contains a safe type of the input.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    List<PairOfDoubleAndVectorOfDoubles> safePairOfDoubleAndVectorOfDoublesList(List<PairOfDoubleAndVectorOfDoubles> input)
    {
        Objects.requireNonNull(input, "Specify a non-null list of ensemble pairs from which to create a safe type.");
        List<PairOfDoubleAndVectorOfDoubles> returnMe = new ArrayList<>();
        input.forEach(value -> {
            if(value instanceof SafePairOfDoubleAndVectorOfDoubles)
            {
                returnMe.add(value);
            }
            else
            {
                returnMe.add(SafePairOfDoubleAndVectorOfDoubles.of(value.getItemOne(), value.getItemTwo()));
            }
        });
        return Collections.unmodifiableList(returnMe);
    }

    /**
     * Returns an immutable list that contains a safe type of the input.
     * 
     * @param input the possibly unsafe input
     * @return the immutable output
     */

    List<VectorOfBooleans> safeVectorOfBooleansList(List<VectorOfBooleans> input)
    {
        Objects.requireNonNull(input,
                               "Specify a non-null list of dichotomous inputs from which to create a safe type.");
        List<VectorOfBooleans> returnMe = new ArrayList<>();
        input.forEach(value -> {
            if(value instanceof SafeVectorOfBooleans)
            {
                returnMe.add(value);
            }
            else
            {
                returnMe.add(SafeVectorOfBooleans.of(value.getBooleans()));
            }
        });
        return Collections.unmodifiableList(returnMe);
    }

    /**
     * Returns a safe type of the input.
     * 
     * @param input the potentially unsafe input
     * @return a safe implementation of the input
     */

    VectorOfDoubles safeVectorOf(VectorOfDoubles input)
    {
        if(input instanceof SafeVectorOfDoubles)
        {
            return input;
        }
        return SafeVectorOfDoubles.of(input.getDoubles());
    }

    /**
     * Returns a safe type of the input.
     * 
     * @param input the potentially unsafe input
     * @return a safe implementation of the input
     */

    MatrixOfDoubles safeMatrixOf(MatrixOfDoubles input)
    {
        if(input instanceof SafeMatrixOfDoubles)
        {
            return input;
        }
        return SafeMatrixOfDoubles.of(input.getDoubles());
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

        @Override
        public boolean equals(Object o)
        {
            if(!(o instanceof SafePairOfBooleans))
            {
                return false;
            }
            SafePairOfBooleans b = (SafePairOfBooleans)o;
            return b.getItemOne() == getItemOne() && b.getItemTwo() == getItemTwo();
        }

        @Override
        public int hashCode()
        {
            return Boolean.hashCode(getItemOne()) + Boolean.hashCode(getItemTwo());
        }

    };

    /**
     * Prevent construction.
     */

    private DefaultDataFactory()
    {
    }

}
