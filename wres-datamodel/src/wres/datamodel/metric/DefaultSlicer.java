package wres.datamodel.metric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubleAndVectorOfDoubles;
import wres.datamodel.PairOfDoubles;

/**
 * Default implementation of a utility class for slicing/dicing and transforming datasets associated with verification
 * metrics. TODO: reconcile this class with the Slicer in wres.datamodel.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class DefaultSlicer implements Slicer
{

    /**
     * Data factory for transformations.
     */

    private final DataFactory dataFac;

    /**
     * Instance of the slicer.
     */

    private static DefaultSlicer instance = null;

    /**
     * Returns an instance of a {@link Slicer}.
     * 
     * @return a {@link DataFactory}
     */

    public static Slicer getInstance()
    {
        if(Objects.isNull(instance))
        {
            instance = new DefaultSlicer();
        }
        return instance;
    }

    /**
     * Null input error message.
     */
    private static final String NULL_INPUT = "Specify a non-null input to transform.";

    /**
     * Null mapper function error message.
     */

    private static final String NULL_MAPPER = "Specify a non-null function to map the input to an output.";

    @Override
    public double[] getLeftSide(SingleValuedPairs input)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        return input.getData().stream().mapToDouble(PairOfDoubles::getItemOne).toArray();
    }

    @Override
    public double[] getLeftSide(EnsemblePairs input)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        return input.getData().stream().mapToDouble(PairOfDoubleAndVectorOfDoubles::getItemOne).toArray();
    }

    @Override
    public double[] getRightSide(SingleValuedPairs input)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        return input.getData().stream().mapToDouble(PairOfDoubles::getItemTwo).toArray();
    }

    @Override
    public SingleValuedPairs sliceByLeft(SingleValuedPairs input, Threshold threshold)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        Objects.requireNonNull(threshold, "Specify a non-null threshold.");
        List<PairOfDoubles> mainPairs = input.getData();
        List<PairOfDoubles> mainPairsSubset = new ArrayList<>();
        mainPairs.forEach(a -> {
            if(threshold.test(a.getItemOne()))
                mainPairsSubset.add(a);
        });
        //No pairs in the subset
        if(mainPairsSubset.isEmpty())
        {
            return null;
        }
        if(input.hasBaseline())
        {
            List<PairOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubles> basePairsSubset = new ArrayList<>();
            basePairs.forEach(a -> {
                if(threshold.test(a.getItemOne()))
                    basePairsSubset.add(a);
            });

            //No pairs in the subset
            if(basePairsSubset.isEmpty())
            {
                return null;
            }
            return dataFac.ofSingleValuedPairs(mainPairsSubset,
                                               basePairsSubset,
                                               input.getMetadata(),
                                               input.getMetadataForBaseline(),
                                               input.getClimatology());
        }
        return dataFac.ofSingleValuedPairs(mainPairsSubset, input.getMetadata());
    }

    @Override
    public EnsemblePairs sliceByLeft(EnsemblePairs input, Threshold threshold)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        Objects.requireNonNull(threshold, "Specify a non-null threshold.");
        List<PairOfDoubleAndVectorOfDoubles> mainPairs = input.getData();
        List<PairOfDoubleAndVectorOfDoubles> mainPairsSubset = new ArrayList<>();
        mainPairs.forEach(a -> {
            if(threshold.test(a.getItemOne()))
                mainPairsSubset.add(a);
        });
        //No pairs in the subset
        if(mainPairsSubset.isEmpty())
        {
            return null;
        }
        if(input.hasBaseline())
        {
            List<PairOfDoubleAndVectorOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubleAndVectorOfDoubles> basePairsSubset = new ArrayList<>();
            basePairs.forEach(a -> {
                if(threshold.test(a.getItemOne()))
                    basePairsSubset.add(a);
            });
            //No pairs in the subset
            if(basePairsSubset.isEmpty())
            {
                return null;
            }
            return dataFac.ofEnsemblePairs(mainPairsSubset,
                                           basePairsSubset,
                                           input.getMetadata(),
                                           input.getMetadataForBaseline(),
                                           input.getClimatology());
        }
        return dataFac.ofEnsemblePairs(mainPairsSubset, input.getMetadata());
    }

    @Override
    public Map<Integer, List<PairOfDoubleAndVectorOfDoubles>> sliceByRight(List<PairOfDoubleAndVectorOfDoubles> input)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        return input.stream().collect(Collectors.groupingBy(pair -> pair.getItemTwo().length));
    }

    @Override
    public Map<MetricConstants, MetricOutputMapByLeadThreshold<ScalarOutput>> sliceByMetricComponent(MetricOutputMapByLeadThreshold<VectorOutput> input)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        Map<MetricConstants, Map<MapBiKey<Integer, Threshold>, ScalarOutput>> sourceMap =
                                                                                        new EnumMap<>(MetricConstants.class);
        MetadataFactory metaFac = dataFac.getMetadataFactory();
        input.forEach((key, value) -> {
            List<MetricConstants> components = value.getOutputTemplate().getMetricComponents();
            for(MetricConstants next: components)
            {
                Map<MapBiKey<Integer, Threshold>, ScalarOutput> nextMap = null;
                if(sourceMap.containsKey(next))
                {
                    nextMap = sourceMap.get(next);
                }
                else
                {
                    nextMap = new HashMap<>();
                    sourceMap.put(next, nextMap);
                }
                //Add the output
                MetricOutputMetadata meta = metaFac.getOutputMetadata(value.getMetadata(), next);
                nextMap.put(key, dataFac.ofScalarOutput(value.getValue(next), meta));
            }
        });
        //Build the scalar results
        Map<MetricConstants, MetricOutputMapByLeadThreshold<ScalarOutput>> returnMe =
                                                                                    new EnumMap<>(MetricConstants.class);
        sourceMap.forEach((key, value) -> returnMe.put(key, dataFac.ofMap(value)));
        return returnMe;
    }

    @Override
    public List<PairOfDoubles> transformPairs(List<PairOfDoubleAndVectorOfDoubles> input,
                                              Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        Objects.requireNonNull(mapper, NULL_MAPPER);
        List<PairOfDoubles> transformed = new ArrayList<>();
        input.stream().map(mapper).forEach(transformed::add);
        return transformed;
    }

    @Override
    public DichotomousPairs transformPairs(SingleValuedPairs input, Function<PairOfDoubles, PairOfBooleans> mapper)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        Objects.requireNonNull(mapper, NULL_MAPPER);
        List<PairOfDoubles> mainPairs = input.getData();
        List<PairOfBooleans> mainPairsTransformed = new ArrayList<>();
        mainPairs.stream().map(mapper).forEach(mainPairsTransformed::add);
        Metadata metaTransformed =
                                 dataFac.getMetadataFactory().getMetadata(input.getMetadata(),
                                                                          dataFac.getMetadataFactory().getDimension());
        if(input.hasBaseline())
        {
            List<PairOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfBooleans> basePairsTransformed = new ArrayList<>();
            basePairs.stream().map(mapper).forEach(basePairsTransformed::add);
            Metadata metaBaseTransformed = dataFac.getMetadataFactory()
                                                  .getMetadata(input.getMetadataForBaseline(),
                                                               dataFac.getMetadataFactory().getDimension());
            return dataFac.ofDichotomousPairsFromAtomic(mainPairsTransformed,
                                                        basePairsTransformed,
                                                        metaTransformed,
                                                        metaBaseTransformed,
                                                        input.getClimatology());
        }
        return dataFac.ofDichotomousPairsFromAtomic(mainPairsTransformed, metaTransformed);
    }

    @Override
    public SingleValuedPairs transformPairs(EnsemblePairs input,
                                            Function<PairOfDoubleAndVectorOfDoubles, PairOfDoubles> mapper)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        Objects.requireNonNull(mapper, NULL_MAPPER);
        List<PairOfDoubles> mainPairsTransformed = transformPairs(input.getData(), mapper);
        if(input.hasBaseline())
        {
            List<PairOfDoubles> basePairsTransformed = transformPairs(input.getDataForBaseline(), mapper);
            return dataFac.ofSingleValuedPairs(mainPairsTransformed,
                                               basePairsTransformed,
                                               input.getMetadata(),
                                               input.getMetadataForBaseline(),
                                               input.getClimatology());
        }
        return dataFac.ofSingleValuedPairs(mainPairsTransformed, input.getMetadata());
    }

    @Override
    public DiscreteProbabilityPairs transformPairs(EnsemblePairs input,
                                                   Threshold threshold,
                                                   BiFunction<PairOfDoubleAndVectorOfDoubles, Threshold, PairOfDoubles> mapper)
    {
        Objects.requireNonNull(input, NULL_INPUT);
        Objects.requireNonNull(mapper, NULL_MAPPER);
        List<PairOfDoubleAndVectorOfDoubles> mainPairs = input.getData();
        List<PairOfDoubles> mainPairsTransformed = new ArrayList<>();
        mainPairs.forEach(pair -> mainPairsTransformed.add(mapper.apply(pair, threshold)));
        if(input.hasBaseline())
        {
            List<PairOfDoubleAndVectorOfDoubles> basePairs = input.getDataForBaseline();
            List<PairOfDoubles> basePairsTransformed = new ArrayList<>();
            basePairs.forEach(pair -> basePairsTransformed.add(mapper.apply(pair, threshold)));
            return dataFac.ofDiscreteProbabilityPairs(mainPairsTransformed,
                                                      basePairsTransformed,
                                                      input.getMetadata(),
                                                      input.getMetadataForBaseline(),
                                                      input.getClimatology());
        }
        return dataFac.ofDiscreteProbabilityPairs(mainPairsTransformed, input.getMetadata());
    }

    @Override
    public PairOfDoubles transformPair(PairOfDoubleAndVectorOfDoubles pair, Threshold threshold)
    {
        Objects.requireNonNull(pair, NULL_INPUT);
        Objects.requireNonNull(threshold, NULL_INPUT);
        double rhs = Arrays.stream(pair.getItemTwo()).map(a -> threshold.test(a) ? 1 : 0).average().getAsDouble();
        return dataFac.pairOf(threshold.test(pair.getItemOne()) ? 1 : 0, rhs);
    }

    @Override
    public PairOfDoubles transformPair(PairOfDoubleAndVectorOfDoubles pair)
    {
        Objects.requireNonNull(pair, NULL_INPUT);
        return dataFac.pairOf(pair.getItemOne(), pair.getItemTwo()[0]);
    }

    @Override
    public double getQuantile(double probability, double[] sorted)
    {
        if(probability < 0 || probability > 1.0)
        {
            throw new IllegalArgumentException("The input probability is not within the unit interval: " + probability);
        }
        if(sorted.length == 0)
        {
            throw new IllegalArgumentException("Cannot compute the inverse cumulative probability from empty input.");
        }
        //Single item
        if(sorted.length == 1)
        {
            return sorted[0];
        }
        //Lower bound
        if(Double.compare(probability, 0) == 0)
        {
            return sorted[0];
        }
        //Upper bound
        if(Double.compare(probability, 1) == 0)
        {
            return sorted[sorted.length - 1];
        }

        //Find the low index, zero-based
        double lowIndex = probability * sorted.length - 1;
        //If the probability maps below the first sample, return the first sample as the lower bound is undefined
        if(lowIndex < 0.0)
        {
            return sorted[0];
        }
        //Otherwise, linearly interpolate between samples
        int lower = (int)Math.floor(lowIndex);
        double fraction = lowIndex - lower;
        return sorted[lower] + fraction * (sorted[lower + 1] - sorted[lower]);
    }

    @Override
    public QuantileThreshold getQuantileFromProbability(ProbabilityThreshold threshold, double[] sorted)
    {
        Objects.requireNonNull(threshold, "Specify a non-null probability threshold.");
        Objects.requireNonNull(sorted, "Specify a non-null array of sorted values.");
        if(sorted.length == 0)
        {
            throw new IllegalArgumentException("Cannot compute the quantile from empty input.");
        }
        Double first = getQuantile(threshold.getThreshold(), sorted);
        Double second = null;
        if(threshold.hasBetweenCondition())
        {
            second = getQuantile(threshold.getThresholdUpper(), sorted);
        }
        return dataFac.getQuantileThreshold(first,
                                            second,
                                            threshold.getThreshold(),
                                            threshold.getThresholdUpper(),
                                            threshold.getCondition());
    }

    /**
     * Hidden constructor.
     */

    private DefaultSlicer()
    {
        dataFac = DefaultDataFactory.getInstance();
    }

}
