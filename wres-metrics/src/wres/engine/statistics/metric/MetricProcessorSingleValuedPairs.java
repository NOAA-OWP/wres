package wres.engine.statistics.metric;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import wres.config.generated.ProjectConfig;
import wres.datamodel.PairOfBooleans;
import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DichotomousPairs;
import wres.datamodel.metric.MatrixOutput;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricConstants.MetricGroup;
import wres.datamodel.metric.MetricOutput;
import wres.datamodel.metric.MetricOutputForProjectByThreshold;
import wres.datamodel.metric.MetricOutputMapByMetric;
import wres.datamodel.metric.MulticategoryPairs;
import wres.datamodel.metric.ProbabilityThreshold;
import wres.datamodel.metric.QuantileThreshold;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.Threshold;

/**
 * Builds and processes all {@link MetricCollection} associated with a {@link ProjectConfig} for metrics that consume
 * {@link SingleValuedPairs} and configured transformations of {@link SingleValuedPairs}. For example, metrics that
 * consume {@link DichotomousPairs} may be processed after transforming the {@link SingleValuedPairs} with an
 * appropriate mapping function.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

final class MetricProcessorSingleValuedPairs extends MetricProcessor<SingleValuedPairs>
{

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     */

    private final MetricCollection<DichotomousPairs, ScalarOutput> dichotomousScalar;

    /**
     * A {@link MetricCollection} of {@link Metric} that consume {@link MulticategoryPairs} and produce
     * {@link MatrixOutput}.
     */

    private final MetricCollection<MulticategoryPairs, MatrixOutput> multicategoryMatrix;

    @Override
    public MetricOutputForProjectByThreshold apply(SingleValuedPairs t)
    {

        //Metric futures
        MetricFutures futures = new MetricFutures();

        //Process the metrics that consume single-valued pairs
        if(hasSingleValuedInput())
        {
            processSingleValuedPairs(t, futures);
        }
        if(hasDichotomousInput())
        {
            processDichotomousPairs(t,futures);
        }

        //Process and return the result        
        return futures.getMetricOutput();
    }

    /**
     * Returns an instance of the processor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     */

    public static MetricProcessorSingleValuedPairs of(DataFactory dataFactory, ProjectConfig config)
    {
        return new MetricProcessorSingleValuedPairs(dataFactory, config);
    }

    @Override
    public boolean hasMetrics(MetricGroup group)
    {
        switch(group)
        {
            case SINGLE_VALUED_SCALAR:
                return Objects.nonNull(singleValuedScalar);
            case SINGLE_VALUED_VECTOR:
                return Objects.nonNull(singleValuedVector);
            case SINGLE_VALUED_MULTIVECTOR:
                return Objects.nonNull(singleValuedMultiVector);    
            case DICHOTOMOUS_SCALAR:
                return Objects.nonNull(dichotomousScalar);
            case MULTICATEGORY_MATRIX:
                return Objects.nonNull(multicategoryMatrix);
            default:
                return false;
        }
    }

    @Override
    public boolean hasScalarOutput()
    {
        return Objects.nonNull(singleValuedScalar) || Objects.nonNull(dichotomousScalar);
    }

    @Override
    public boolean hasVectorOutput()
    {
        return Objects.nonNull(singleValuedVector);
    }

    @Override
    public boolean hasMultiVectorOutput()
    {
        return Objects.nonNull(singleValuedMultiVector);
    }

    @Override
    public boolean hasMatrixOutput()
    {
        return Objects.nonNull(multicategoryMatrix);
    }

    @Override
    public boolean hasDichotomousInput()
    {
        return Objects.nonNull(dichotomousScalar);
    }

    @Override
    public boolean hasMulticategoryInput()
    {
        return Objects.nonNull(multicategoryMatrix);
    }

    @Override
    public boolean hasDiscreteProbabilityInput()
    {
        return false;
    }

    @Override
    public boolean hasEnsembleInput()
    {
        return false;
    }

    /**
     * Processes a set of metric futures that consume {@link DichotomousPairs}, which are mapped from the input pairs,
     * {@link SingleValuedPairs}, using a configured mapping function.
     * 
     * @param input the input pairs
     * @param futures the metric futures
     */

    void processDichotomousPairs(SingleValuedPairs input, MetricFutures futures)
    {

        //Metric-specific overrides are currently unsupported
        String unsupportedException = "Metric-specific threshold overrides are currently unsupported.";
        //Check and obtain the global thresholds by metric group for iteration
        if(hasMetrics(MetricGroup.DICHOTOMOUS_SCALAR))
        {
            //Deal with global thresholds
            if(hasGlobalThresholds(MetricGroup.DICHOTOMOUS_SCALAR))
            {
                List<Threshold> global = globalThresholds.get(MetricGroup.DICHOTOMOUS_SCALAR);
                double[] sorted = getSortedLeftSide(input, global);
                global.forEach(threshold -> processDichotomousThreshold(threshold,
                                                                        sorted,
                                                                        input,
                                                                        dichotomousScalar,
                                                                        futures.scalar));
            }
            //Deal with metric-local thresholds
            else
            {
                //Hook for future logic
                throw new UnsupportedOperationException(unsupportedException);
            }
        }
    }

    /**
     * Builds a metric future for a {@link MetricCollection} that consumes {@link DichotomousPairs} at a specific
     * {@link Threshold} and appends it to the input map of futures.
     * 
     * @param threshold the threshold
     * @param sorted a sorted set of values from which to determine {@link QuantileThreshold} where the input
     *            {@link Threshold} is a {@link ProbabilityThreshold}.
     * @param pairs the pairs
     * @param collection the metric collection
     * @param futures the collection of futures to which the new future will be added
     */

    private <T extends MetricOutput<?>> void processDichotomousThreshold(Threshold threshold,
                                                                         double[] sorted,
                                                                         SingleValuedPairs pairs,
                                                                         MetricCollection<DichotomousPairs, T> collection,
                                                                         Map<Threshold, CompletableFuture<MetricOutputMapByMetric<T>>> futures)
    {
        Threshold useMe = threshold;
        //Quantile required: need to determine real-value from probability
        if(threshold instanceof ProbabilityThreshold)
        {
            useMe = dataFactory.getSlicer().getQuantileFromProbability((ProbabilityThreshold)useMe, sorted);
        }

        //Define a mapper to convert the single-valued pairs to dichotomous pairs
        final Threshold finalThreshold = useMe;
        Function<PairOfDoubles, PairOfBooleans> mapper =
                                                       pair -> dataFactory.pairOf(finalThreshold.test(pair.getItemOne()),
                                                                                  finalThreshold.test(pair.getItemTwo()));
        //Slice the pairs
        DichotomousPairs transformed = dataFactory.getSlicer().transformPairs(pairs, mapper);
        futures.put(useMe, CompletableFuture.supplyAsync(() -> collection.apply(transformed)));
    }

    /**
     * Hidden constructor.
     * 
     * @param dataFactory the data factory
     * @param config the project configuration
     */

    private MetricProcessorSingleValuedPairs(DataFactory dataFactory, ProjectConfig config)
    {
        super(dataFactory, config);
        //Validate the configuration
        if(getMetricsFromConfig(config, MetricGroup.DICHOTOMOUS_SCALAR).length > 0)
        {
            throw new IllegalArgumentException("Cannot configure dichotomous metrics for ensemble inputs: correct the configuration '"
                + config.getLabel() + "'.");
        }
        if(getMetricsFromConfig(config, MetricGroup.MULTICATEGORY_MATRIX).length > 0)
        {
            throw new IllegalArgumentException("Cannot configure multicategory metrics for ensemble inputs: correct the configuration '"
                + config.getLabel() + "'.");
        }
        //Construct the metrics
        dichotomousScalar = ofDichotomousScalarCollection(config);
        multicategoryMatrix = ofMulticategoryMatrixCollection(config);
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput} or null if no such metrics exist within the input {@link ProjectConfig}.
     * 
     * @param config the project configuration
     * @return a collection of metrics or null
     */

    private MetricCollection<DichotomousPairs, ScalarOutput> ofDichotomousScalarCollection(ProjectConfig config)
    {
        //Obtain the list of metrics and find the matching metrics 
        MetricConstants[] metrics = getMetricsFromConfig(config, MetricGroup.DICHOTOMOUS_SCALAR);
        return metrics.length == 0 ? null : metricFactory.ofDichotomousScalarCollection(metrics);
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link MulticategoryPairs} and produce
     * {@link MatrixOutput} or null if no such metrics exist within the input {@link ProjectConfig}.
     * 
     * @param config the project configuration
     * @return a collection of metrics or null
     */

    private MetricCollection<MulticategoryPairs, MatrixOutput> ofMulticategoryMatrixCollection(ProjectConfig config)
    {
        //Obtain the list of metrics and find the matching metrics 
        MetricConstants[] metrics = getMetricsFromConfig(config, MetricGroup.MULTICATEGORY_MATRIX);
        return metrics.length == 0 ? null : metricFactory.ofMulticategoryMatrixCollection(metrics);
    }

}
