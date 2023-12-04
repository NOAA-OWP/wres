package wres.pipeline;


import java.util.concurrent.ExecutorService;

/**
 * A value object that reduces count of args for some methods and provides names for those objects.
 *
 * @param readingExecutor the executor for reading data sources
 * @param ingestExecutor the executor for ingesting data into a database
 * @param poolExecutor the executor for completing pools
 * @param slicingExecutor the executor for slicing/dicing/transforming datasets
 * @param metricExecutor the executor for computing metrics
 * @param productExecutor the executor for writing products or formats
 * @param samplingUncertaintyExecutor the executor for calculating sampling uncertainties
 */

record EvaluationExecutors( ExecutorService readingExecutor,
                            ExecutorService ingestExecutor,
                            ExecutorService poolExecutor,
                            ExecutorService slicingExecutor,
                            ExecutorService metricExecutor,
                            ExecutorService productExecutor,
                            ExecutorService samplingUncertaintyExecutor )
{
}