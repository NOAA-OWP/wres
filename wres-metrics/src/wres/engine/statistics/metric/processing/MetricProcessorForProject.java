package wres.engine.statistics.metric.processing;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import wres.config.generated.DatasourceType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.MetricOutputAccessException;
import wres.datamodel.outputs.MetricOutputForProjectByTimeAndThreshold;
import wres.engine.statistics.metric.MetricFactory;

/**
 * Helper class that collates concrete metric processors together.
 * 
 * @author james.brown@hydrosolved.com
 * @since 0.1
 * @version 0.4
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
     * @param canonicalThresholds an optional set of canonical thresholds (one per metric group), may be null
     * @param thresholdExecutor an executor service for processing thresholds
     * @param metricExecutor an executor service for processing metrics
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorForProject( final MetricFactory metricFactory,
                                      final ProjectConfig projectConfig,
                                      final List<Set<Threshold>> canonicalThresholds,
                                      final ExecutorService thresholdExecutor,
                                      final ExecutorService metricExecutor )
            throws MetricProcessorException
    {
        DatasourceType type = projectConfig.getInputs().getRight().getType();
        if ( type.equals( DatasourceType.SINGLE_VALUED_FORECASTS ) || type.equals( DatasourceType.SIMULATIONS ) )
        {
            singleValuedProcessor = metricFactory.ofMetricProcessorByTimeSingleValuedPairs( projectConfig,
                                                                                            canonicalThresholds,
                                                                                            thresholdExecutor,
                                                                                            metricExecutor );
            ensembleProcessor = null;
        }
        else
        {
            ensembleProcessor = metricFactory.ofMetricProcessorByTimeEnsemblePairs( projectConfig,
                                                                                    canonicalThresholds,
                                                                                    thresholdExecutor,
                                                                                    metricExecutor );
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

    public Set<MetricOutputGroup> getCachedMetricOutputTypes()
    {
        if ( Objects.nonNull( singleValuedProcessor ) )
        {
            return singleValuedProcessor.getMetricOutputToCache();
        }
        else
        {
            return ensembleProcessor.getMetricOutputToCache();
        }
    }

    /**
     * Returns the cached metric output or null if {@link #hasCachedMetricOutput()} returns <code>false</code>.
     * 
     * @return the cached output or null
     * @throws MetricOutputAccessException if the metric output could not be accessed
     */

    public MetricOutputForProjectByTimeAndThreshold getCachedMetricOutput() throws MetricOutputAccessException
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
