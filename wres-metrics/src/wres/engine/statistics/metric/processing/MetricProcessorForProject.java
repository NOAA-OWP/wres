package wres.engine.statistics.metric.processing;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import wres.config.MetricConfigException;
import wres.config.generated.DatasourceType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutputException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.MetricFactory;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.config.MetricConfigHelper;

/**
 * Helper class that collates concrete metric processors together.
 * 
 * @author james.brown@hydrosolved.com
 */

public class MetricProcessorForProject
{
    /**
     * Processor for {@link EnsemblePairs}.
     */

    private final MetricProcessorByTime<EnsemblePairs> ensembleProcessor;

    /**
     * Processor for {@link SingleValuedPairs}.
     */

    private final MetricProcessorByTime<SingleValuedPairs> singleValuedProcessor;

    /**
     * Build the processor collection.
     * 
     * @param metricFactory an instance of a metric factory
     * @param projectConfig the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an executor service for processing thresholds
     * @param metricExecutor an executor service for processing metrics
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     * @throws MetricConfigException if the metric configuration is incorrect
     */

    public MetricProcessorForProject( final MetricFactory metricFactory,
                                      final ProjectConfig projectConfig,
                                      final ThresholdsByMetric externalThresholds,
                                      final ExecutorService thresholdExecutor,
                                      final ExecutorService metricExecutor )
            throws MetricParameterException
    {
        DatasourceType type = projectConfig.getInputs().getRight().getType();

        Set<MetricOutputGroup> mergeTheseResults = MetricConfigHelper.getCacheListFromProjectConfig( projectConfig );

        if ( type.equals( DatasourceType.SINGLE_VALUED_FORECASTS ) || type.equals( DatasourceType.SIMULATIONS ) )
        {
            singleValuedProcessor = metricFactory.ofMetricProcessorByTimeSingleValuedPairs( projectConfig,
                                                                                            externalThresholds,
                                                                                            thresholdExecutor,
                                                                                            metricExecutor,
                                                                                            mergeTheseResults );
            ensembleProcessor = null;
        }
        else
        {
            ensembleProcessor = metricFactory.ofMetricProcessorByTimeEnsemblePairs( projectConfig,
                                                                                    externalThresholds,
                                                                                    thresholdExecutor,
                                                                                    metricExecutor,
                                                                                    mergeTheseResults );
            singleValuedProcessor = null;
        }
    }

    /**
     * Returns a {@link MetricProcessorByTime} for {@link SingleValuedPairs} or throws an exception if this 
     * {@link MetricProcessorForProject} was not constructed to process {@link SingleValuedPairs}.
     * 
     * @return a single-valued metric processor
     * @throws MetricProcessorException if this {@link MetricProcessorForProject} was not constructed to process 
     *            {@link SingleValuedPairs}
     */

    public MetricProcessorByTime<SingleValuedPairs> getMetricProcessorForSingleValuedPairs()
            throws MetricProcessorException
    {
        if ( Objects.isNull( singleValuedProcessor ) )
        {
            throw new MetricProcessorException( "This metric processor was not built to consume single-valued pairs." );
        }
        return singleValuedProcessor;
    }

    /**
     * Returns a {@link MetricProcessorByTime} for {@link EnsemblePairs} or throws an exception if this 
     * {@link MetricProcessorForProject} was not constructed to process {@link EnsemblePairs}.
     * 
     * @return a single-valued metric processor
     * @throws MetricProcessorException if this {@link MetricProcessorForProject} was not constructed to process 
     *            {@link EnsemblePairs}
     */

    public MetricProcessorByTime<EnsemblePairs> getMetricProcessorForEnsemblePairs()
            throws MetricProcessorException
    {
        if ( Objects.isNull( ensembleProcessor ) )
        {
            throw new MetricProcessorException( "This metric processor was not built to consume ensemble pairs." );
        }
        return ensembleProcessor;
    }

    /**
     * Returns <code>true</code> if the processor contains cached output, otherwise <code>false</code>.
     * 
     * @return true if the processor contains cached output, otherwise false.
     */

    public boolean hasCachedMetricOutput()
    {
        if ( Objects.nonNull( singleValuedProcessor ) )
        {
            return singleValuedProcessor.hasCachedMetricOutput();
        }
        else
        {
            return ensembleProcessor.hasCachedMetricOutput();
        }
    }

    /**
     * Returns the set of {@link MetricOutputGroup} that will be cached across successive executions of a 
     * {@link MetricProcessor}.
     * 
     * @return the output types to cache
     */

    public Set<MetricOutputGroup> getMetricOutputTypesToCache()
    {
        if ( Objects.nonNull( singleValuedProcessor ) )
        {
            return singleValuedProcessor.getMetricOutputTypesToCache();
        }
        else
        {
            return ensembleProcessor.getMetricOutputTypesToCache();
        }
    }

    /**
     * Returns the set of {@link MetricOutputGroup} that were actually cached across successive executions of a 
     * {@link MetricProcessor}. This may differ from {@link #getMetricOutputTypesToCache()}, as some end-of-pipeline 
     * outputs are computed and cached automatically.
     * 
     * @return the output types to cache
     * @throws InterruptedException if the retrieval was interrupted
     * @throws MetricOutputException if the output could not be retrieved
     */

    public Set<MetricOutputGroup> getCachedMetricOutputTypes() throws InterruptedException
    {
        if ( Objects.nonNull( singleValuedProcessor ) )
        {
            return singleValuedProcessor.getCachedMetricOutputTypes();
        }
        else
        {
            return ensembleProcessor.getCachedMetricOutputTypes();
        }
    }

    /**
     * Returns the cached metric output or null if {@link #hasCachedMetricOutput()} returns <code>false</code>.
     * 
     * @return the cached output or null
     * @throws InterruptedException if the retrieval was interrupted
     * @throws MetricOutputException if the output could not be retrieved
     */

    public MetricOutputForProjectByTimeAndThreshold getCachedMetricOutput() throws InterruptedException
    {
        if ( Objects.nonNull( singleValuedProcessor ) )
        {
            return singleValuedProcessor.getCachedMetricOutput();
        }
        else
        {
            return ensembleProcessor.getCachedMetricOutput();
        }
    }

}
