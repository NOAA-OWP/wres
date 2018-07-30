package wres.engine.statistics.metric;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.inputs.pairs.TimeSeriesOfSingleValuedPairs;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.categorical.ContingencyTable;
import wres.engine.statistics.metric.categorical.EquitableThreatScore;
import wres.engine.statistics.metric.categorical.FrequencyBias;
import wres.engine.statistics.metric.categorical.PeirceSkillScore;
import wres.engine.statistics.metric.categorical.ProbabilityOfDetection;
import wres.engine.statistics.metric.categorical.ProbabilityOfFalseDetection;
import wres.engine.statistics.metric.categorical.ThreatScore;
import wres.engine.statistics.metric.discreteprobability.BrierScore;
import wres.engine.statistics.metric.discreteprobability.BrierSkillScore;
import wres.engine.statistics.metric.discreteprobability.RelativeOperatingCharacteristicDiagram;
import wres.engine.statistics.metric.discreteprobability.RelativeOperatingCharacteristicScore;
import wres.engine.statistics.metric.discreteprobability.ReliabilityDiagram;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByForecast;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByObserved;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilityScore;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilitySkillScore;
import wres.engine.statistics.metric.ensemble.RankHistogram;
import wres.engine.statistics.metric.processing.MetricProcessor;
import wres.engine.statistics.metric.processing.MetricProcessorByTime;
import wres.engine.statistics.metric.processing.MetricProcessorByTimeEnsemblePairs;
import wres.engine.statistics.metric.processing.MetricProcessorByTimeSingleValuedPairs;
import wres.engine.statistics.metric.processing.MetricProcessorForProject;
import wres.engine.statistics.metric.singlevalued.BiasFraction;
import wres.engine.statistics.metric.singlevalued.CoefficientOfDetermination;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons;
import wres.engine.statistics.metric.singlevalued.IndexOfAgreement;
import wres.engine.statistics.metric.singlevalued.KlingGuptaEfficiency;
import wres.engine.statistics.metric.singlevalued.MeanAbsoluteError;
import wres.engine.statistics.metric.singlevalued.MeanError;
import wres.engine.statistics.metric.singlevalued.MeanSquareError;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;
import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareError;
import wres.engine.statistics.metric.singlevalued.SumOfSquareError;
import wres.engine.statistics.metric.singlevalued.VolumetricEfficiency;
import wres.engine.statistics.metric.timeseries.TimeToPeakError;
import wres.engine.statistics.metric.timeseries.TimeToPeakRelativeError;

/**
 * <p>A factory class for constructing metrics.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetricFactory
{

    /**
     * String used in several error messages to denote an unrecognized metric.
     */

    private static final String UNRECOGNIZED_METRIC_ERROR = "Unrecognized metric for identifier.";

    /**
     * Returns a {@link MetricProcessorForProject} for the specified project configuration.
     * 
     * @param projectConfig the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an executor service for processing thresholds
     * @param metricExecutor an executor service for processing metrics
     * @return a metric processor
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessorForProject ofMetricProcessorForProject( final ProjectConfig projectConfig,
                                                                         final ThresholdsByMetric externalThresholds,
                                                                         final ExecutorService thresholdExecutor,
                                                                         final ExecutorService metricExecutor )
            throws MetricParameterException
    {
        return new MetricProcessorForProject( projectConfig,
                                              externalThresholds,
                                              thresholdExecutor,
                                              metricExecutor );
    }

    /**
     * <p>Returns an instance of a {@link MetricProcessor} for processing {@link SingleValuedPairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param config the project configuration
     * @param mergeSet an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final Set<MetricOutputGroup> mergeSet )
                    throws MetricParameterException
    {
        return MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                       null,
                                                                       ForkJoinPool.commonPool(),
                                                                       ForkJoinPool.commonPool(),
                                                                       mergeSet );
    }

    /**
     * <p>Returns an instance of a {@link MetricProcessor} for processing {@link EnsemblePairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param config the project configuration
     * @param mergeSet an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessorByTime<EnsemblePairs>
            ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                  final Set<MetricOutputGroup> mergeSet )
                    throws MetricParameterException
    {
        return MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config,
                                                                   null,
                                                                   ForkJoinPool.commonPool(),
                                                                   ForkJoinPool.commonPool(),
                                                                   mergeSet );
    }

    /**
     * <p>Returns an instance of a {@link MetricProcessor} for processing {@link SingleValuedPairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param mergeSet an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final ThresholdsByMetric externalThresholds,
                                                      final Set<MetricOutputGroup> mergeSet )
                    throws MetricParameterException
    {
        return MetricFactory.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                       externalThresholds,
                                                                       ForkJoinPool.commonPool(),
                                                                       ForkJoinPool.commonPool(),
                                                                       mergeSet );
    }

    /**
     * <p>Returns an instance of a {@link MetricProcessor} for processing {@link EnsemblePairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds (one per metric), may be null
     * @param mergeSet an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessorByTime<EnsemblePairs>
            ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                  final ThresholdsByMetric externalThresholds,
                                                  final Set<MetricOutputGroup> mergeSet )
                    throws MetricParameterException
    {
        return MetricFactory.ofMetricProcessorByTimeEnsemblePairs( config,
                                                                   externalThresholds,
                                                                   ForkJoinPool.commonPool(),
                                                                   ForkJoinPool.commonPool(),
                                                                   mergeSet );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link SingleValuedPairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeSet an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final ThresholdsByMetric externalThresholds,
                                                      final ExecutorService thresholdExecutor,
                                                      final ExecutorService metricExecutor,
                                                      final Set<MetricOutputGroup> mergeSet )
                    throws MetricParameterException
    {
        return new MetricProcessorByTimeSingleValuedPairs( config,
                                                           externalThresholds,
                                                           thresholdExecutor,
                                                           metricExecutor,
                                                           mergeSet );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link EnsemblePairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeSet an optional set of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessorByTime<EnsemblePairs> ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                                                             final ThresholdsByMetric externalThresholds,
                                                                                             final ExecutorService thresholdExecutor,
                                                                                             final ExecutorService metricExecutor,
                                                                                             final Set<MetricOutputGroup> mergeSet )
            throws MetricParameterException
    {
        return new MetricProcessorByTimeEnsemblePairs( config,
                                                       externalThresholds,
                                                       thresholdExecutor,
                                                       metricExecutor,
                                                       mergeSet );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link DoubleScoreOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
            ofSingleValuedScoreCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<SingleValuedPairs, MultiVectorOutput, MultiVectorOutput>
            ofSingleValuedMultiVectorCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return MetricFactory.ofSingleValuedMultiVectorCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link DoubleScoreOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<DiscreteProbabilityPairs, DoubleScoreOutput, DoubleScoreOutput>
            ofDiscreteProbabilityScoreCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return MetricFactory.ofDiscreteProbabilityScoreCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link DoubleScoreOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<DichotomousPairs, MatrixOutput, DoubleScoreOutput>
            ofDichotomousScoreCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return MetricFactory.ofDichotomousScoreCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link MultiVectorOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput, MultiVectorOutput>
            ofDiscreteProbabilityMultiVectorCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return MetricFactory.ofDiscreteProbabilityMultiVectorCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link MatrixOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<DichotomousPairs, MatrixOutput, MatrixOutput>
            ofDichotomousMatrixCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return MetricFactory.ofDichotomousMatrixCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link DoubleScoreOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static MetricCollection<EnsemblePairs, DoubleScoreOutput, DoubleScoreOutput>
            ofEnsembleScoreCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return MetricFactory.ofEnsembleScoreCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link MultiVectorOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<EnsemblePairs, MultiVectorOutput, MultiVectorOutput>
            ofEnsembleMultiVectorCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return MetricFactory.ofEnsembleMultiVectorCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link BoxPlotOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<EnsemblePairs, BoxPlotOutput, BoxPlotOutput>
            ofEnsembleBoxPlotCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return MetricFactory.ofEnsembleBoxPlotCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link TimeSeriesOfSingleValuedPairs} and produce
     * {@link PairedOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>>
            ofSingleValuedTimeSeriesCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return MetricFactory.ofSingleValuedTimeSeriesCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link DoubleScoreOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
            ofSingleValuedScoreCollection( ExecutorService executor,
                                           MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput> builder =
                MetricCollectionBuilder.of();

        // Add the metrics
        for ( MetricConstants next : metric )
        {
            if ( MetricFactory.isCollectable( next ) )
            {
                builder.addCollectable( MetricFactory.ofSingleValuedScoreCollectable( next ) );
            }
            else
            {
                builder.addMetric( MetricFactory.ofSingleValuedScore( next ) );
            }
        }

        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static MetricCollection<SingleValuedPairs, MultiVectorOutput, MultiVectorOutput>
            ofSingleValuedMultiVectorCollection( ExecutorService executor,
                                                 MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<SingleValuedPairs, MultiVectorOutput, MultiVectorOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofSingleValuedMultiVector( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link DoubleScoreOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<DiscreteProbabilityPairs, DoubleScoreOutput, DoubleScoreOutput>
            ofDiscreteProbabilityScoreCollection( ExecutorService executor,
                                                  MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, DoubleScoreOutput, DoubleScoreOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofDiscreteProbabilityScore( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link DoubleScoreOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static MetricCollection<DichotomousPairs, MatrixOutput, DoubleScoreOutput>
            ofDichotomousScoreCollection( ExecutorService executor,
                                          MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DichotomousPairs, MatrixOutput, DoubleScoreOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            // All dichotomous scores are collectable
            builder.addCollectable( MetricFactory.ofDichotomousScore( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput, MultiVectorOutput>
            ofDiscreteProbabilityMultiVectorCollection( ExecutorService executor,
                                                        MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, MultiVectorOutput, MultiVectorOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofDiscreteProbabilityMultiVector( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link MatrixOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static MetricCollection<DichotomousPairs, MatrixOutput, MatrixOutput>
            ofDichotomousMatrixCollection( ExecutorService executor,
                                           MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DichotomousPairs, MatrixOutput, MatrixOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofDichotomousMatrix( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link DoubleScoreOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<EnsemblePairs, DoubleScoreOutput, DoubleScoreOutput>
            ofEnsembleScoreCollection( ExecutorService executor,
                                       MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, DoubleScoreOutput, DoubleScoreOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofEnsembleScore( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<EnsemblePairs, MultiVectorOutput, MultiVectorOutput>
            ofEnsembleMultiVectorCollection( ExecutorService executor,
                                             MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, MultiVectorOutput, MultiVectorOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofEnsembleMultiVector( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link BoxPlotOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<EnsemblePairs, BoxPlotOutput, BoxPlotOutput>
            ofEnsembleBoxPlotCollection( ExecutorService executor,
                                         MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, BoxPlotOutput, BoxPlotOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofEnsembleBoxPlot( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link TimeSeriesOfSingleValuedPairs} and produce
     * {@link PairedOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>>
            ofSingleValuedTimeSeriesCollection( ExecutorService executor,
                                                MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofSingleValuedTimeSeries( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link DoubleScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return the metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<SingleValuedPairs, DoubleScoreOutput> ofSingleValuedScore( MetricConstants metric )
    {
        switch ( metric )
        {
            case BIAS_FRACTION:
                return BiasFraction.of();
            case KLING_GUPTA_EFFICIENCY:
                return KlingGuptaEfficiency.of();
            case MEAN_ABSOLUTE_ERROR:
                return MeanAbsoluteError.of();
            case MEAN_ERROR:
                return MeanError.of();
            case SAMPLE_SIZE:
                return SampleSize.of();
            case INDEX_OF_AGREEMENT:
                return IndexOfAgreement.of();
            case VOLUMETRIC_EFFICIENCY:
                return VolumetricEfficiency.of();
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return MeanSquareErrorSkillScore.of();
            case COEFFICIENT_OF_DETERMINATION:
                return CoefficientOfDetermination.of();
            case PEARSON_CORRELATION_COEFFICIENT:
                return CorrelationPearsons.of();
            case MEAN_SQUARE_ERROR:
                return MeanSquareError.of();
            case ROOT_MEAN_SQUARE_ERROR:
                return RootMeanSquareError.of();
            case SUM_OF_SQUARE_ERROR:
                return SumOfSquareError.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Collectable} that consumes {@link SingleValuedPairs} and produces {@link DoubleScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return the metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
            ofSingleValuedScoreCollectable( MetricConstants metric )
    {
        switch ( metric )
        {
            case SUM_OF_SQUARE_ERROR:
                return SumOfSquareError.of();
            case COEFFICIENT_OF_DETERMINATION:
                return CoefficientOfDetermination.of();
            case PEARSON_CORRELATION_COEFFICIENT:
                return CorrelationPearsons.of();
            case MEAN_SQUARE_ERROR:
                return MeanSquareError.of();
            case ROOT_MEAN_SQUARE_ERROR:
                return RootMeanSquareError.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<SingleValuedPairs, MultiVectorOutput> ofSingleValuedMultiVector( MetricConstants metric )
    {
        if ( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.equals( metric ) )
        {
            return QuantileQuantileDiagram.of();
        }
        else
        {
            throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DiscreteProbabilityPairs} and produces {@link DoubleScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<DiscreteProbabilityPairs, DoubleScoreOutput>
            ofDiscreteProbabilityScore( MetricConstants metric )
    {
        switch ( metric )
        {
            case BRIER_SCORE:
                return BrierScore.of();
            case BRIER_SKILL_SCORE:
                return BrierSkillScore.of();
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE:
                return RelativeOperatingCharacteristicScore.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DichotomousPairs} and produces {@link DoubleScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Collectable<DichotomousPairs, MatrixOutput, DoubleScoreOutput>
            ofDichotomousScore( MetricConstants metric )
    {
        switch ( metric )
        {
            case THREAT_SCORE:
                return ThreatScore.of();
            case EQUITABLE_THREAT_SCORE:
                return EquitableThreatScore.of();
            case PEIRCE_SKILL_SCORE:
                return PeirceSkillScore.of();
            case PROBABILITY_OF_DETECTION:
                return ProbabilityOfDetection.of();
            case PROBABILITY_OF_FALSE_DETECTION:
                return ProbabilityOfFalseDetection.of();
            case FREQUENCY_BIAS:
                return FrequencyBias.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link MulticategoryPairs} and produces {@link DoubleScoreOutput}. Use
     * {@link #ofDichotomousScore(MetricConstants)} when the inputs are dichotomous.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<MulticategoryPairs, DoubleScoreOutput> ofMulticategoryScore( MetricConstants metric )
    {
        if ( MetricConstants.PEIRCE_SKILL_SCORE.equals( metric ) )
        {
            return PeirceSkillScore.of();
        }
        else
        {
            throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DiscreteProbabilityPairs} and produces {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<DiscreteProbabilityPairs, MultiVectorOutput>
            ofDiscreteProbabilityMultiVector( MetricConstants metric ) throws MetricParameterException
    {
        switch ( metric )
        {
            case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                return RelativeOperatingCharacteristicDiagram.of();
            case RELIABILITY_DIAGRAM:
                return ReliabilityDiagram.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DichotomousPairs} and produces {@link MatrixOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<DichotomousPairs, MatrixOutput> ofDichotomousMatrix( MetricConstants metric )
    {
        if ( MetricConstants.CONTINGENCY_TABLE.equals( metric ) )
        {
            return ContingencyTable.of();
        }
        else
        {
            throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link DoubleScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<EnsemblePairs, DoubleScoreOutput> ofEnsembleScore( MetricConstants metric )
    {
        switch ( metric )
        {
            case CONTINUOUS_RANKED_PROBABILITY_SCORE:
                return ContinuousRankedProbabilityScore.of();
            case CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return ContinuousRankedProbabilitySkillScore.of();
            case SAMPLE_SIZE:
                return SampleSize.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link BoxPlotOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<EnsemblePairs, BoxPlotOutput> ofEnsembleBoxPlot( MetricConstants metric )
    {
        switch ( metric )
        {
            case BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE:
                return BoxPlotErrorByObserved.of();
            case BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE:
                return BoxPlotErrorByForecast.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<EnsemblePairs, MultiVectorOutput> ofEnsembleMultiVector( MetricConstants metric )
    {
        if ( MetricConstants.RANK_HISTOGRAM.equals( metric ) )
        {
            return RankHistogram.of();
        }
        else
        {
            throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link TimeSeriesOfSingleValuedPairs} and produces {@link PairedOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
            ofSingleValuedTimeSeries( MetricConstants metric )
    {
        switch ( metric )
        {
            case TIME_TO_PEAK_ERROR:
                return TimeToPeakError.of();
            case TIME_TO_PEAK_RELATIVE_ERROR:
                return TimeToPeakRelativeError.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Helper that returns the name of the summary statistics associated with the timing metric or null if no 
     * summary statistics are defined for the specified input.
     * 
     * @param timingMetric the named timing metric
     * @return the summary statistics name or null if no identifier is defined
     * @throws NullPointerException if the input is null
     */

    public static MetricConstants ofSummaryStatisticsForTimingErrorMetric( MetricConstants timingMetric )
    {
        Objects.requireNonNull( timingMetric, "Specify a non-null metric identifier to map." );

        if ( timingMetric == MetricConstants.TIME_TO_PEAK_ERROR )
        {
            return MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC;
        }

        if ( timingMetric == MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR )
        {
            return MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC;
        }

        return null;
    }

    /**
     * Returns <code>true</code> if the input metric is an instance of {@link Collectable}, otherwise 
     * <code>false</code>.
     * 
     * @param metric the metric
     * @return true if the metric is {@link Collectable}, otherwise false
     */

    private static boolean isCollectable( MetricConstants metric )
    {
        Objects.requireNonNull( metric, "Specify a non-null metric to test." );

        boolean singleValued = metric == MetricConstants.COEFFICIENT_OF_DETERMINATION
                               || metric == MetricConstants.PEARSON_CORRELATION_COEFFICIENT
                               || metric == MetricConstants.SUM_OF_SQUARE_ERROR;

        singleValued = singleValued
                       || metric == MetricConstants.MEAN_SQUARE_ERROR
                       || metric == MetricConstants.ROOT_MEAN_SQUARE_ERROR;

        return singleValued || metric.isInGroup( MetricInputGroup.DICHOTOMOUS );
    }

    /**
     * Hidden constructor.
     * 
     * @param outputFactory a {@link DataFactory}
     */

    private MetricFactory()
    {
    }

}
