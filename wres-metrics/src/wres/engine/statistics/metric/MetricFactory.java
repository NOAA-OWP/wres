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
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInput;
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
import wres.engine.statistics.metric.SampleSize.SampleSizeBuilder;
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
import wres.engine.statistics.metric.discreteprobability.RelativeOperatingCharacteristicDiagram.RelativeOperatingCharacteristicBuilder;
import wres.engine.statistics.metric.discreteprobability.RelativeOperatingCharacteristicScore;
import wres.engine.statistics.metric.discreteprobability.RelativeOperatingCharacteristicScore.RelativeOperatingCharacteristicScoreBuilder;
import wres.engine.statistics.metric.discreteprobability.ReliabilityDiagram;
import wres.engine.statistics.metric.discreteprobability.ReliabilityDiagram.ReliabilityDiagramBuilder;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByForecast;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByForecast.BoxPlotErrorByForecastBuilder;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByObserved;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByObserved.BoxPlotErrorByObservedBuilder;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilityScore;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilityScore.CRPSBuilder;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilitySkillScore;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilitySkillScore.CRPSSBuilder;
import wres.engine.statistics.metric.ensemble.RankHistogram;
import wres.engine.statistics.metric.ensemble.RankHistogram.RankHistogramBuilder;
import wres.engine.statistics.metric.processing.MetricProcessor;
import wres.engine.statistics.metric.processing.MetricProcessorByTime;
import wres.engine.statistics.metric.processing.MetricProcessorByTimeEnsemblePairs;
import wres.engine.statistics.metric.processing.MetricProcessorByTimeSingleValuedPairs;
import wres.engine.statistics.metric.processing.MetricProcessorForProject;
import wres.engine.statistics.metric.singlevalued.BiasFraction;
import wres.engine.statistics.metric.singlevalued.BiasFraction.BiasFractionBuilder;
import wres.engine.statistics.metric.singlevalued.CoefficientOfDetermination;
import wres.engine.statistics.metric.singlevalued.CoefficientOfDetermination.CoefficientOfDeterminationBuilder;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons.CorrelationPearsonsBuilder;
import wres.engine.statistics.metric.singlevalued.IndexOfAgreement;
import wres.engine.statistics.metric.singlevalued.IndexOfAgreement.IndexOfAgreementBuilder;
import wres.engine.statistics.metric.singlevalued.KlingGuptaEfficiency;
import wres.engine.statistics.metric.singlevalued.KlingGuptaEfficiency.KlingGuptaEfficiencyBuilder;
import wres.engine.statistics.metric.singlevalued.MeanAbsoluteError;
import wres.engine.statistics.metric.singlevalued.MeanAbsoluteError.MeanAbsoluteErrorBuilder;
import wres.engine.statistics.metric.singlevalued.MeanError;
import wres.engine.statistics.metric.singlevalued.MeanError.MeanErrorBuilder;
import wres.engine.statistics.metric.singlevalued.MeanSquareError;
import wres.engine.statistics.metric.singlevalued.MeanSquareError.MeanSquareErrorBuilder;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder;
import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareError;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareError.RootMeanSquareErrorBuilder;
import wres.engine.statistics.metric.singlevalued.SumOfSquareError;
import wres.engine.statistics.metric.singlevalued.SumOfSquareError.SumOfSquareErrorBuilder;
import wres.engine.statistics.metric.singlevalued.VolumetricEfficiency;
import wres.engine.statistics.metric.singlevalued.VolumetricEfficiency.VolumetricEfficiencyBuilder;
import wres.engine.statistics.metric.timeseries.TimeToPeakError;
import wres.engine.statistics.metric.timeseries.TimeToPeakError.TimeToPeakErrorBuilder;
import wres.engine.statistics.metric.timeseries.TimeToPeakRelativeError;
import wres.engine.statistics.metric.timeseries.TimeToPeakRelativeError.TimeToPeakRelativeErrorBuilder;
import wres.engine.statistics.metric.timeseries.TimingErrorDurationStatistics;
import wres.engine.statistics.metric.timeseries.TimingErrorDurationStatistics.TimingErrorDurationStatisticsBuilder;

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
        return ofSingleValuedScoreCollection( ForkJoinPool.commonPool(), metric );
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
        return ofSingleValuedMultiVectorCollection( ForkJoinPool.commonPool(), metric );
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
        return ofDiscreteProbabilityScoreCollection( ForkJoinPool.commonPool(), metric );
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
        return ofDichotomousScoreCollection( ForkJoinPool.commonPool(), metric );
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
        return ofDiscreteProbabilityMultiVectorCollection( ForkJoinPool.commonPool(), metric );
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
        return ofDichotomousMatrixCollection( ForkJoinPool.commonPool(), metric );
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
        return ofEnsembleScoreCollection( ForkJoinPool.commonPool(), metric );
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
        return ofEnsembleMultiVectorCollection( ForkJoinPool.commonPool(), metric );
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
        return ofEnsembleBoxPlotCollection( ForkJoinPool.commonPool(), metric );
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

    public static MetricCollection<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>>
            ofSingleValuedTimeSeriesCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return ofSingleValuedTimeSeriesCollection( ForkJoinPool.commonPool(), metric );
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

    public static MetricCollection<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>>
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
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<SingleValuedPairs, DoubleScoreOutput> ofSingleValuedScore( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case BIAS_FRACTION:
                return MetricFactory.ofBiasFraction();
            case KLING_GUPTA_EFFICIENCY:
                return MetricFactory.ofKlingGuptaEfficiency();
            case MEAN_ABSOLUTE_ERROR:
                return MetricFactory.ofMeanAbsoluteError();
            case MEAN_ERROR:
                return MetricFactory.ofMeanError();
            case SAMPLE_SIZE:
                return MetricFactory.ofSampleSize();
            case INDEX_OF_AGREEMENT:
                return MetricFactory.ofIndexOfAgreement();
            case VOLUMETRIC_EFFICIENCY:
                return MetricFactory.ofVolumetricEfficiency();
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return MetricFactory.ofMeanSquareErrorSkillScore();
            case COEFFICIENT_OF_DETERMINATION:
                return MetricFactory.ofCoefficientOfDetermination();
            case PEARSON_CORRELATION_COEFFICIENT:
                return MetricFactory.ofCorrelationPearsons();
            case MEAN_SQUARE_ERROR:
                return MetricFactory.ofMeanSquareError();
            case ROOT_MEAN_SQUARE_ERROR:
                return MetricFactory.ofRootMeanSquareError();
            case SUM_OF_SQUARE_ERROR:
                return MetricFactory.ofSumOfSquareError();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Collectable} that consumes {@link SingleValuedPairs} and produces {@link DoubleScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return the metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
            ofSingleValuedScoreCollectable( MetricConstants metric )
                    throws MetricParameterException
    {
        switch ( metric )
        {
            case SUM_OF_SQUARE_ERROR:
                return MetricFactory.ofSumOfSquareError();
            case COEFFICIENT_OF_DETERMINATION:
                return MetricFactory.ofCoefficientOfDetermination();
            case PEARSON_CORRELATION_COEFFICIENT:
                return MetricFactory.ofCorrelationPearsons();
            case MEAN_SQUARE_ERROR:
                return MetricFactory.ofMeanSquareError();
            case ROOT_MEAN_SQUARE_ERROR:
                return MetricFactory.ofRootMeanSquareError();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<SingleValuedPairs, MultiVectorOutput> ofSingleValuedMultiVector( MetricConstants metric )
            throws MetricParameterException
    {
        if ( MetricConstants.QUANTILE_QUANTILE_DIAGRAM.equals( metric ) )
        {
            return ofQuantileQuantileDiagram();
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
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<DiscreteProbabilityPairs, DoubleScoreOutput>
            ofDiscreteProbabilityScore( MetricConstants metric )
                    throws MetricParameterException
    {
        switch ( metric )
        {
            case BRIER_SCORE:
                return MetricFactory.ofBrierScore();
            case BRIER_SKILL_SCORE:
                return MetricFactory.ofBrierSkillScore();
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE:
                return MetricFactory.ofRelativeOperatingCharacteristicScore();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DichotomousPairs} and produces {@link DoubleScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Collectable<DichotomousPairs, MatrixOutput, DoubleScoreOutput> ofDichotomousScore( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case THREAT_SCORE:
                return MetricFactory.ofThreatScore();
            case EQUITABLE_THREAT_SCORE:
                return MetricFactory.ofEquitableThreatScore();
            case PEIRCE_SKILL_SCORE:
                return MetricFactory.ofPeirceSkillScore();
            case PROBABILITY_OF_DETECTION:
                return MetricFactory.ofProbabilityOfDetection();
            case PROBABILITY_OF_FALSE_DETECTION:
                return MetricFactory.ofProbabilityOfFalseDetection();
            case FREQUENCY_BIAS:
                return MetricFactory.ofFrequencyBias();
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
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<MulticategoryPairs, DoubleScoreOutput> ofMulticategoryScore( MetricConstants metric )
            throws MetricParameterException
    {
        if ( MetricConstants.PEIRCE_SKILL_SCORE.equals( metric ) )
        {
            return ofPeirceSkillScore();
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
                return MetricFactory.ofRelativeOperatingCharacteristic();
            case RELIABILITY_DIAGRAM:
                return MetricFactory.ofReliabilityDiagram();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DichotomousPairs} and produces {@link MatrixOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<DichotomousPairs, MatrixOutput> ofDichotomousMatrix( MetricConstants metric )
            throws MetricParameterException
    {
        if ( MetricConstants.CONTINGENCY_TABLE.equals( metric ) )
        {
            return ofDichotomousContingencyTable();
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
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<EnsemblePairs, DoubleScoreOutput> ofEnsembleScore( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case CONTINUOUS_RANKED_PROBABILITY_SCORE:
                return MetricFactory.ofContinuousRankedProbabilityScore();
            case CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return MetricFactory.ofContinuousRankedProbabilitySkillScore();
            case SAMPLE_SIZE:
                return MetricFactory.ofSampleSize();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link BoxPlotOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<EnsemblePairs, BoxPlotOutput> ofEnsembleBoxPlot( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE:
                return MetricFactory.ofBoxPlotErrorByObserved();
            case BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE:
                return MetricFactory.ofBoxPlotErrorByForecast();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<EnsemblePairs, MultiVectorOutput> ofEnsembleMultiVector( MetricConstants metric )
            throws MetricParameterException
    {
        if ( MetricConstants.RANK_HISTOGRAM.equals( metric ) )
        {
            return ofRankHistogram();
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
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
            ofSingleValuedTimeSeries( MetricConstants metric )
                    throws MetricParameterException
    {
        switch ( metric )
        {
            case TIME_TO_PEAK_ERROR:
                return MetricFactory.ofTimeToPeakError();
            case TIME_TO_PEAK_RELATIVE_ERROR:
                return MetricFactory.ofTimeToPeakRelativeError();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Return a default {@link BiasFraction} function.
     * 
     * @return a default {@link BiasFraction} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static BiasFraction ofBiasFraction() throws MetricParameterException
    {
        return (BiasFraction) new BiasFractionBuilder().build();
    }

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static BrierScore ofBrierScore() throws MetricParameterException
    {
        return (BrierScore) new BrierScore.BrierScoreBuilder().build();
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static BrierSkillScore ofBrierSkillScore() throws MetricParameterException
    {
        return (BrierSkillScore) new BrierSkillScore.BrierSkillScoreBuilder().build();
    }

    /**
     * Return a default {@link CoefficientOfDetermination} function.
     * 
     * @return a default {@link CoefficientOfDetermination} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static CoefficientOfDetermination ofCoefficientOfDetermination() throws MetricParameterException
    {
        return (CoefficientOfDetermination) new CoefficientOfDeterminationBuilder().build();
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static ContingencyTable<DichotomousPairs> ofDichotomousContingencyTable() throws MetricParameterException
    {
        return new ContingencyTable.ContingencyTableBuilder<DichotomousPairs>().build();
    }

    /**
     * Return a default {@link CorrelationPearsons} function.
     * 
     * @return a default {@link CorrelationPearsons} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static CorrelationPearsons ofCorrelationPearsons() throws MetricParameterException
    {
        return (CorrelationPearsons) new CorrelationPearsonsBuilder().build();
    }

    /**
     * Return a default {@link ThreatScore} function.
     * 
     * @return a default {@link ThreatScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static ThreatScore ofThreatScore() throws MetricParameterException
    {
        return (ThreatScore) new ThreatScore.ThreatScoreBuilder().build();
    }

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static EquitableThreatScore ofEquitableThreatScore() throws MetricParameterException
    {
        return (EquitableThreatScore) new EquitableThreatScore.EquitableThreatScoreBuilder().build();
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static MeanAbsoluteError ofMeanAbsoluteError() throws MetricParameterException
    {
        return (MeanAbsoluteError) new MeanAbsoluteErrorBuilder().build();
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static MeanError ofMeanError() throws MetricParameterException
    {
        return (MeanError) new MeanErrorBuilder().build();
    }

    /**
     * Return a default {@link SumOfSquareError} function.
     * 
     * @return a default {@link SumOfSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static SumOfSquareError ofSumOfSquareError() throws MetricParameterException
    {
        return new SumOfSquareErrorBuilder().build();
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static MeanSquareError ofMeanSquareError() throws MetricParameterException
    {
        return new MeanSquareErrorBuilder().build();
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static MeanSquareErrorSkillScore ofMeanSquareErrorSkillScore() throws MetricParameterException
    {
        return new MeanSquareErrorSkillScoreBuilder().build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function.
     * 
     * @param <S> the type of pairs
     * @return a default {@link PeirceSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static <S extends MulticategoryPairs> PeirceSkillScore<S> ofPeirceSkillScore() throws MetricParameterException
    {
        return (PeirceSkillScore<S>) new PeirceSkillScore.PeirceSkillScoreBuilder<S>().build();
    }

    /**
     * Return a default {@link ProbabilityOfDetection} function.
     * 
     * @return a default {@link ProbabilityOfDetection} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static ProbabilityOfDetection ofProbabilityOfDetection() throws MetricParameterException
    {
        return (ProbabilityOfDetection) new ProbabilityOfDetection.ProbabilityOfDetectionBuilder().build();
    }

    /**
     * Return a default {@link ProbabilityOfFalseDetection} function.
     * 
     * @return a default {@link ProbabilityOfFalseDetection} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static ProbabilityOfFalseDetection ofProbabilityOfFalseDetection() throws MetricParameterException
    {
        return (ProbabilityOfFalseDetection) new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder().build();
    }

    /**
     * Return a default {@link QuantileQuantileDiagram} function.
     * 
     * @return a default {@link QuantileQuantileDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static QuantileQuantileDiagram ofQuantileQuantileDiagram() throws MetricParameterException
    {
        return (QuantileQuantileDiagram) new QuantileQuantileDiagram.QuantileQuantileDiagramBuilder().build();
    }

    /**
     * Return a default {@link RootMeanSquareError} function.
     * 
     * @return a default {@link RootMeanSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public static RootMeanSquareError ofRootMeanSquareError() throws MetricParameterException
    {
        return (RootMeanSquareError) new RootMeanSquareErrorBuilder().build();
    }

    /**
     * Return a default {@link ReliabilityDiagram} function.
     * 
     * @return a default {@link ReliabilityDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static ReliabilityDiagram ofReliabilityDiagram() throws MetricParameterException
    {
        return (ReliabilityDiagram) new ReliabilityDiagramBuilder().build();
    }

    /**
     * Return a default {@link RelativeOperatingCharacteristicDiagram} function.
     * 
     * @return a default {@link RelativeOperatingCharacteristicDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static RelativeOperatingCharacteristicDiagram ofRelativeOperatingCharacteristic() throws MetricParameterException
    {
        return (RelativeOperatingCharacteristicDiagram) new RelativeOperatingCharacteristicBuilder().build();
    }

    /**
     * Return a default {@link RelativeOperatingCharacteristicScore} function.
     * 
     * @return a default {@link RelativeOperatingCharacteristicScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static RelativeOperatingCharacteristicScore ofRelativeOperatingCharacteristicScore() throws MetricParameterException
    {
        return (RelativeOperatingCharacteristicScore) new RelativeOperatingCharacteristicScoreBuilder().build();
    }

    /**
     * Return a default {@link ContinuousRankedProbabilityScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilityScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static ContinuousRankedProbabilityScore ofContinuousRankedProbabilityScore() throws MetricParameterException
    {
        return (ContinuousRankedProbabilityScore) new CRPSBuilder().build();
    }

    /**
     * Return a default {@link ContinuousRankedProbabilitySkillScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilitySkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static ContinuousRankedProbabilitySkillScore ofContinuousRankedProbabilitySkillScore()
            throws MetricParameterException
    {
        return (ContinuousRankedProbabilitySkillScore) new CRPSSBuilder().build();
    }

    /**
     * Return a default {@link IndexOfAgreement} function.
     * 
     * @return a default {@link IndexOfAgreement} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static IndexOfAgreement ofIndexOfAgreement() throws MetricParameterException
    {
        return (IndexOfAgreement) new IndexOfAgreementBuilder().build();
    }

    /**
     * Return a default {@link KlingGuptaEfficiency} function.
     * 
     * @return a default {@link KlingGuptaEfficiency} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static KlingGuptaEfficiency ofKlingGuptaEfficiency() throws MetricParameterException
    {
        return (KlingGuptaEfficiency) new KlingGuptaEfficiencyBuilder().build();
    }

    /**
     * Return a default {@link SampleSize} function.
     * 
     * @param <T> the type of {@link MetricInput}
     * @return a default {@link SampleSize} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static <T extends MetricInput<?>> SampleSize<T> ofSampleSize() throws MetricParameterException
    {
        return (SampleSize<T>) new SampleSizeBuilder<T>().build();
    }

    /**
     * Return a default {@link RankHistogram} function.
     * 
     * @return a default {@link RankHistogram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static RankHistogram ofRankHistogram() throws MetricParameterException
    {
        return (RankHistogram) new RankHistogramBuilder().build();
    }

    /**
     * Return a default {@link FrequencyBias} function.
     * 
     * @return a default {@link FrequencyBias} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static FrequencyBias ofFrequencyBias() throws MetricParameterException
    {
        return (FrequencyBias) new FrequencyBias.FrequencyBiasBuilder().build();
    }

    /**
     * Return a default {@link BoxPlotErrorByObserved} function.
     * 
     * @return a default {@link BoxPlotErrorByObserved} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static BoxPlotErrorByObserved ofBoxPlotErrorByObserved() throws MetricParameterException
    {
        return (BoxPlotErrorByObserved) new BoxPlotErrorByObservedBuilder().build();
    }

    /**
     * Return a default {@link BoxPlotErrorByForecast} function.
     * 
     * @return a default {@link BoxPlotErrorByForecast} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static BoxPlotErrorByForecast ofBoxPlotErrorByForecast() throws MetricParameterException
    {
        return (BoxPlotErrorByForecast) new BoxPlotErrorByForecastBuilder().build();
    }

    /**
     * Return a default {@link VolumetricEfficiency} function.
     * 
     * @return a default {@link VolumetricEfficiency} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static VolumetricEfficiency ofVolumetricEfficiency() throws MetricParameterException
    {
        return (VolumetricEfficiency) new VolumetricEfficiencyBuilder().build();
    }

    /**
     * Return a default {@link TimeToPeakError} function.
     * 
     * @return a default {@link TimeToPeakError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static TimeToPeakError ofTimeToPeakError() throws MetricParameterException
    {
        return (TimeToPeakError) new TimeToPeakErrorBuilder().build();
    }

    /**
     * Return a default {@link TimeToPeakRelativeError} function.
     * 
     * @return a default {@link TimeToPeakRelativeError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static TimeToPeakRelativeError ofTimeToPeakRelativeError() throws MetricParameterException
    {
        return (TimeToPeakRelativeError) new TimeToPeakRelativeErrorBuilder().build();
    }

    /**
     * Return a {@link TimingErrorDurationStatistics} function for a prescribed set of {@link MetricConstants}.
     * For each of the {@link MetricConstants}, the {@link MetricConstants#isInGroup(ScoreOutputGroup)} should return 
     * <code>true</code> when supplied with {@link ScoreOutputGroup#UNIVARIATE_STATISTIC}.
     * 
     * @param identifier the named metric for which summary statistics are required
     * @param statistics the identifiers for summary statistics
     * @return a default {@link TimingErrorDurationStatistics} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public static TimingErrorDurationStatistics ofTimingErrorDurationStatistics( MetricConstants identifier,
                                                                          Set<MetricConstants> statistics )
            throws MetricParameterException
    {
        return new TimingErrorDurationStatisticsBuilder().setStatistics( statistics )
                                                         .setID( identifier )
                                                         .build();
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
