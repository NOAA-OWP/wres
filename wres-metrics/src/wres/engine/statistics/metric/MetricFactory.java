package wres.engine.statistics.metric;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricInputGroup;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.ThresholdsByMetric;
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
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.SampleSize.SampleSizeBuilder;
import wres.engine.statistics.metric.categorical.ContingencyTable;
import wres.engine.statistics.metric.categorical.EquitableThreatScore;
import wres.engine.statistics.metric.categorical.FrequencyBias;
import wres.engine.statistics.metric.categorical.PeirceSkillScore;
import wres.engine.statistics.metric.categorical.ProbabilityOfDetection;
import wres.engine.statistics.metric.categorical.ProbabilityOfFalseDetection;
import wres.engine.statistics.metric.categorical.ThreatScore;
import wres.engine.statistics.metric.config.MetricConfigHelper;
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
import wres.engine.statistics.metric.processing.MetricProcessorException;
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

public class MetricFactory
{

    /**
     * Instance of the factory.
     */

    private static MetricFactory instance = null;

    /**
     * String used in several error messages to denote an unrecognized metric.
     */

    private static final String UNRECOGNIZED_METRIC_ERROR = "Unrecognized metric for identifier.";

    /**
     * String used in several error messages to denote a configuration error.
     */

    private static final String CONFIGURATION_ERROR =
            "While building the metric processor, a configuration exception occurred: ";

    /**
     * String used in several error messages to denote a parameter error.
     */

    private static final String PARAMETER_ERROR =
            "While building the metric processor, a parameter exception occurred: ";

    /**
     * Instance of an {@link DataFactory} for building metric outputs.
     */

    private final DataFactory outputFactory;

    /**
     * Returns an instance of a {@link MetricFactory}.
     * 
     * @param dataFactory a {@link DataFactory}
     * @return a {@link MetricFactory}
     */

    public static MetricFactory getInstance( final DataFactory dataFactory )
    {
        //Lazy construction
        if ( Objects.isNull( instance ) )
        {
            instance = new MetricFactory( dataFactory );
        }
        return instance;
    }

    /**
     * Returns a {@link MetricProcessorForProject} for the specified project configuration.
     * 
     * @param projectConfig the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an executor service for processing thresholds
     * @param metricExecutor an executor service for processing metrics
     * @return a metric processor
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorForProject ofMetricProcessorForProject( final ProjectConfig projectConfig,
                                                                  final ThresholdsByMetric externalThresholds,
                                                                  final ExecutorService thresholdExecutor,
                                                                  final ExecutorService metricExecutor )
            throws MetricProcessorException
    {
        return new MetricProcessorForProject( this,
                                              projectConfig,
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
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final Set<MetricOutputGroup> mergeSet )
                    throws MetricProcessorException
    {
        return this.ofMetricProcessorByTimeSingleValuedPairs( config,
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
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs>
            ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                  final Set<MetricOutputGroup> mergeSet )
                    throws MetricProcessorException
    {
        return this.ofMetricProcessorByTimeEnsemblePairs( config,
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
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final ThresholdsByMetric externalThresholds,
                                                      final Set<MetricOutputGroup> mergeSet )
                    throws MetricProcessorException
    {
        return this.ofMetricProcessorByTimeSingleValuedPairs( config,
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
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs>
            ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                  final ThresholdsByMetric externalThresholds,
                                                  final Set<MetricOutputGroup> mergeSet )
                    throws MetricProcessorException
    {
        return this.ofMetricProcessorByTimeEnsemblePairs( config,
                                                          externalThresholds,
                                                          ForkJoinPool.commonPool(),
                                                          ForkJoinPool.commonPool(),
                                                          mergeSet );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link SingleValuedPairs}. Uses the input 
     * project configuration to determine which results should be merged and cached across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final ThresholdsByMetric externalThresholds,
                                                      final ExecutorService thresholdExecutor,
                                                      final ExecutorService metricExecutor )
                    throws MetricProcessorException
    {
        try
        {
            return this.ofMetricProcessorByTimeSingleValuedPairs( config,
                                                                  externalThresholds,
                                                                  thresholdExecutor,
                                                                  metricExecutor,
                                                                  MetricFactory.getCacheListFromProjectConfig( config ) );
        }
        catch ( MetricConfigException e )
        {
            throw new MetricProcessorException( CONFIGURATION_ERROR, e );
        }
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link EnsemblePairs}. Uses the input 
     * project configuration to determine which results should be merged and cached across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds, may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs> ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                                                      final ThresholdsByMetric externalThresholds,
                                                                                      final ExecutorService thresholdExecutor,
                                                                                      final ExecutorService metricExecutor )
            throws MetricProcessorException
    {
        try
        {
            return this.ofMetricProcessorByTimeEnsemblePairs( config,
                                                              externalThresholds,
                                                              thresholdExecutor,
                                                              metricExecutor,
                                                              MetricFactory.getCacheListFromProjectConfig( config ) );
        }
        catch ( MetricConfigException e )
        {
            throw new MetricProcessorException( CONFIGURATION_ERROR, e );
        }
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
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final ThresholdsByMetric externalThresholds,
                                                      final ExecutorService thresholdExecutor,
                                                      final ExecutorService metricExecutor,
                                                      final Set<MetricOutputGroup> mergeSet )
                    throws MetricProcessorException
    {
        try
        {
            return new MetricProcessorByTimeSingleValuedPairs( outputFactory,
                                                               config,
                                                               externalThresholds,
                                                               thresholdExecutor,
                                                               metricExecutor,
                                                               mergeSet );
        }
        catch ( MetricConfigException e )
        {
            throw new MetricProcessorException( CONFIGURATION_ERROR, e );
        }
        catch ( MetricParameterException e )
        {
            throw new MetricProcessorException( PARAMETER_ERROR, e );
        }
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
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs> ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                                                      final ThresholdsByMetric externalThresholds,
                                                                                      final ExecutorService thresholdExecutor,
                                                                                      final ExecutorService metricExecutor,
                                                                                      final Set<MetricOutputGroup> mergeSet )
            throws MetricProcessorException
    {
        try
        {
            return new MetricProcessorByTimeEnsemblePairs( outputFactory,
                                                           config,
                                                           externalThresholds,
                                                           thresholdExecutor,
                                                           metricExecutor,
                                                           mergeSet );
        }
        catch ( MetricConfigException e )
        {
            throw new MetricProcessorException( CONFIGURATION_ERROR, e );
        }
        catch ( MetricParameterException e )
        {
            throw new MetricProcessorException( PARAMETER_ERROR, e );
        }
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

    public MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
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

    public MetricCollection<SingleValuedPairs, MultiVectorOutput, MultiVectorOutput>
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

    public MetricCollection<DiscreteProbabilityPairs, DoubleScoreOutput, DoubleScoreOutput>
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

    public MetricCollection<DichotomousPairs, MatrixOutput, DoubleScoreOutput>
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

    public MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput, MultiVectorOutput>
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

    public MetricCollection<DichotomousPairs, MatrixOutput, MatrixOutput>
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

    public MetricCollection<EnsemblePairs, DoubleScoreOutput, DoubleScoreOutput>
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

    public MetricCollection<EnsemblePairs, MultiVectorOutput, MultiVectorOutput>
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

    public MetricCollection<EnsemblePairs, BoxPlotOutput, BoxPlotOutput>
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

    public MetricCollection<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>>
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

    public MetricCollection<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
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
                builder.addCollectable( this.ofSingleValuedScoreCollectable( next ) );
            }
            else
            {
                builder.addMetric( this.ofSingleValuedScore( next ) );
            }
        }

        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<SingleValuedPairs, MultiVectorOutput, MultiVectorOutput>
            ofSingleValuedMultiVectorCollection( ExecutorService executor,
                                                 MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<SingleValuedPairs, MultiVectorOutput, MultiVectorOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( this.ofSingleValuedMultiVector( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<DiscreteProbabilityPairs, DoubleScoreOutput, DoubleScoreOutput>
            ofDiscreteProbabilityScoreCollection( ExecutorService executor,
                                                  MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, DoubleScoreOutput, DoubleScoreOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( this.ofDiscreteProbabilityScore( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<DichotomousPairs, MatrixOutput, DoubleScoreOutput>
            ofDichotomousScoreCollection( ExecutorService executor,
                                          MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DichotomousPairs, MatrixOutput, DoubleScoreOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            // All dichotomous scores are collectable
            builder.addCollectable( this.ofDichotomousScore( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput, MultiVectorOutput>
            ofDiscreteProbabilityMultiVectorCollection( ExecutorService executor,
                                                        MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, MultiVectorOutput, MultiVectorOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( this.ofDiscreteProbabilityMultiVector( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<DichotomousPairs, MatrixOutput, MatrixOutput>
            ofDichotomousMatrixCollection( ExecutorService executor,
                                           MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DichotomousPairs, MatrixOutput, MatrixOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( this.ofDichotomousMatrix( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<EnsemblePairs, DoubleScoreOutput, DoubleScoreOutput>
            ofEnsembleScoreCollection( ExecutorService executor,
                                       MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, DoubleScoreOutput, DoubleScoreOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( this.ofEnsembleScore( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<EnsemblePairs, MultiVectorOutput, MultiVectorOutput>
            ofEnsembleMultiVectorCollection( ExecutorService executor,
                                             MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, MultiVectorOutput, MultiVectorOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( this.ofEnsembleMultiVector( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<EnsemblePairs, BoxPlotOutput, BoxPlotOutput>
            ofEnsembleBoxPlotCollection( ExecutorService executor,
                                         MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, BoxPlotOutput, BoxPlotOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( this.ofEnsembleBoxPlot( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public MetricCollection<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>>
            ofSingleValuedTimeSeriesCollection( ExecutorService executor,
                                                MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>, PairedOutput<Instant, Duration>> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( this.ofSingleValuedTimeSeries( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
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

    public Metric<SingleValuedPairs, DoubleScoreOutput> ofSingleValuedScore( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case BIAS_FRACTION:
                return this.ofBiasFraction();
            case KLING_GUPTA_EFFICIENCY:
                return this.ofKlingGuptaEfficiency();
            case MEAN_ABSOLUTE_ERROR:
                return this.ofMeanAbsoluteError();
            case MEAN_ERROR:
                return this.ofMeanError();
            case SAMPLE_SIZE:
                return this.ofSampleSize();
            case INDEX_OF_AGREEMENT:
                return this.ofIndexOfAgreement();
            case VOLUMETRIC_EFFICIENCY:
                return this.ofVolumetricEfficiency();
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return this.ofMeanSquareErrorSkillScore();
            case COEFFICIENT_OF_DETERMINATION:
                return this.ofCoefficientOfDetermination();
            case PEARSON_CORRELATION_COEFFICIENT:
                return this.ofCorrelationPearsons();
            case MEAN_SQUARE_ERROR:
                return this.ofMeanSquareError();
            case ROOT_MEAN_SQUARE_ERROR:
                return this.ofRootMeanSquareError();
            case SUM_OF_SQUARE_ERROR:
                return this.ofSumOfSquareError();
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

    public Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>
            ofSingleValuedScoreCollectable( MetricConstants metric )
                    throws MetricParameterException
    {
        switch ( metric )
        {
            case SUM_OF_SQUARE_ERROR:
                return this.ofSumOfSquareError();
            case COEFFICIENT_OF_DETERMINATION:
                return this.ofCoefficientOfDetermination();
            case PEARSON_CORRELATION_COEFFICIENT:
                return this.ofCorrelationPearsons();
            case MEAN_SQUARE_ERROR:
                return this.ofMeanSquareError();
            case ROOT_MEAN_SQUARE_ERROR:
                return this.ofRootMeanSquareError();
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

    public Metric<SingleValuedPairs, MultiVectorOutput> ofSingleValuedMultiVector( MetricConstants metric )
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

    public Metric<DiscreteProbabilityPairs, DoubleScoreOutput>
            ofDiscreteProbabilityScore( MetricConstants metric )
                    throws MetricParameterException
    {
        switch ( metric )
        {
            case BRIER_SCORE:
                return this.ofBrierScore();
            case BRIER_SKILL_SCORE:
                return this.ofBrierSkillScore();
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE:
                return this.ofRelativeOperatingCharacteristicScore();
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

    public Collectable<DichotomousPairs, MatrixOutput, DoubleScoreOutput> ofDichotomousScore( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case THREAT_SCORE:
                return this.ofThreatScore();
            case EQUITABLE_THREAT_SCORE:
                return this.ofEquitableThreatScore();
            case PEIRCE_SKILL_SCORE:
                return this.ofPeirceSkillScore();
            case PROBABILITY_OF_DETECTION:
                return this.ofProbabilityOfDetection();
            case PROBABILITY_OF_FALSE_DETECTION:
                return this.ofProbabilityOfFalseDetection();
            case FREQUENCY_BIAS:
                return this.ofFrequencyBias();
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

    public Metric<MulticategoryPairs, DoubleScoreOutput> ofMulticategoryScore( MetricConstants metric )
            throws MetricParameterException
    {
        if ( MetricConstants.PEIRCE_SKILL_SCORE.equals( metric ) )
        {
            return ofPeirceSkillScoreMulti();
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

    public Metric<DiscreteProbabilityPairs, MultiVectorOutput>
            ofDiscreteProbabilityMultiVector( MetricConstants metric ) throws MetricParameterException
    {
        switch ( metric )
        {
            case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                return this.ofRelativeOperatingCharacteristic();
            case RELIABILITY_DIAGRAM:
                return this.ofReliabilityDiagram();
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

    public Metric<DichotomousPairs, MatrixOutput> ofDichotomousMatrix( MetricConstants metric )
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

    public Metric<EnsemblePairs, DoubleScoreOutput> ofEnsembleScore( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case CONTINUOUS_RANKED_PROBABILITY_SCORE:
                return this.ofContinuousRankedProbabilityScore();
            case CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return this.ofContinuousRankedProbabilitySkillScore();
            case SAMPLE_SIZE:
                return this.ofSampleSize();
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

    public Metric<EnsemblePairs, BoxPlotOutput> ofEnsembleBoxPlot( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE:
                return this.ofBoxPlotErrorByObserved();
            case BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE:
                return this.ofBoxPlotErrorByForecast();
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

    public Metric<EnsemblePairs, MultiVectorOutput> ofEnsembleMultiVector( MetricConstants metric )
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

    public Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>
            ofSingleValuedTimeSeries( MetricConstants metric )
                    throws MetricParameterException
    {
        switch ( metric )
        {
            case TIME_TO_PEAK_ERROR:
                return this.ofTimeToPeakError();
            case TIME_TO_PEAK_RELATIVE_ERROR:
                return this.ofTimeToPeakRelativeError();
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

    public BiasFraction ofBiasFraction() throws MetricParameterException
    {
        return (BiasFraction) new BiasFractionBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BrierScore ofBrierScore() throws MetricParameterException
    {
        return (BrierScore) new BrierScore.BrierScoreBuilder().setOutputFactory( outputFactory )
                                                              .build();
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BrierSkillScore ofBrierSkillScore() throws MetricParameterException
    {
        return (BrierSkillScore) new BrierSkillScore.BrierSkillScoreBuilder().setOutputFactory( outputFactory )
                                                                             .build();
    }

    /**
     * Return a default {@link CoefficientOfDetermination} function.
     * 
     * @return a default {@link CoefficientOfDetermination} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public CoefficientOfDetermination ofCoefficientOfDetermination() throws MetricParameterException
    {
        return (CoefficientOfDetermination) new CoefficientOfDeterminationBuilder().setOutputFactory( outputFactory )
                                                                                   .build();
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContingencyTable<DichotomousPairs> ofDichotomousContingencyTable() throws MetricParameterException
    {
        return new ContingencyTable.ContingencyTableBuilder<DichotomousPairs>().setOutputFactory( outputFactory )
                                                                               .build();
    }

    /**
     * Return a default {@link CorrelationPearsons} function.
     * 
     * @return a default {@link CorrelationPearsons} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public CorrelationPearsons ofCorrelationPearsons() throws MetricParameterException
    {
        return (CorrelationPearsons) new CorrelationPearsonsBuilder().setOutputFactory( outputFactory )
                                                                     .build();
    }

    /**
     * Return a default {@link ThreatScore} function.
     * 
     * @return a default {@link ThreatScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public ThreatScore ofThreatScore() throws MetricParameterException
    {
        return (ThreatScore) new ThreatScore.ThreatScoreBuilder().setOutputFactory( outputFactory )
                                                                 .build();
    }

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public EquitableThreatScore ofEquitableThreatScore() throws MetricParameterException
    {
        return (EquitableThreatScore) new EquitableThreatScore.EquitableThreatScoreBuilder().setOutputFactory( outputFactory )
                                                                                            .build();
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public MeanAbsoluteError ofMeanAbsoluteError() throws MetricParameterException
    {
        return (MeanAbsoluteError) new MeanAbsoluteErrorBuilder().setOutputFactory( outputFactory )
                                                                 .build();
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public MeanError ofMeanError() throws MetricParameterException
    {
        return (MeanError) new MeanErrorBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link SumOfSquareError} function.
     * 
     * @return a default {@link SumOfSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public SumOfSquareError<SingleValuedPairs> ofSumOfSquareError() throws MetricParameterException
    {
        return (SumOfSquareError<SingleValuedPairs>) new SumOfSquareErrorBuilder<>().setOutputFactory( outputFactory )
                                                                                    .build();
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public MeanSquareError<SingleValuedPairs> ofMeanSquareError() throws MetricParameterException
    {
        return (MeanSquareError<SingleValuedPairs>) new MeanSquareErrorBuilder<>().setOutputFactory( outputFactory )
                                                                                  .build();
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public MeanSquareErrorSkillScore<SingleValuedPairs> ofMeanSquareErrorSkillScore() throws MetricParameterException
    {
        return (MeanSquareErrorSkillScore<SingleValuedPairs>) new MeanSquareErrorSkillScoreBuilder<>().setOutputFactory( outputFactory )
                                                                                                      .build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a dichotomous event.
     * 
     * @return a default {@link PeirceSkillScore} function for a dichotomous event
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public PeirceSkillScore<DichotomousPairs> ofPeirceSkillScore() throws MetricParameterException
    {
        return (PeirceSkillScore<DichotomousPairs>) new PeirceSkillScore.PeirceSkillScoreBuilder<DichotomousPairs>().setOutputFactory( outputFactory )
                                                                                                                    .build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a multicategory event.
     * 
     * @return a default {@link PeirceSkillScore} function for a multicategory event
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public PeirceSkillScore<MulticategoryPairs> ofPeirceSkillScoreMulti() throws MetricParameterException
    {
        return (PeirceSkillScore<MulticategoryPairs>) new PeirceSkillScore.PeirceSkillScoreBuilder<MulticategoryPairs>().setOutputFactory( outputFactory )
                                                                                                                        .build();
    }

    /**
     * Return a default {@link ProbabilityOfDetection} function.
     * 
     * @return a default {@link ProbabilityOfDetection} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ProbabilityOfDetection ofProbabilityOfDetection() throws MetricParameterException
    {
        return (ProbabilityOfDetection) new ProbabilityOfDetection.ProbabilityOfDetectionBuilder().setOutputFactory( outputFactory )
                                                                                                  .build();
    }

    /**
     * Return a default {@link ProbabilityOfFalseDetection} function.
     * 
     * @return a default {@link ProbabilityOfFalseDetection} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ProbabilityOfFalseDetection ofProbabilityOfFalseDetection() throws MetricParameterException
    {
        return (ProbabilityOfFalseDetection) new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder().setOutputFactory( outputFactory )
                                                                                                                 .build();
    }

    /**
     * Return a default {@link QuantileQuantileDiagram} function.
     * 
     * @return a default {@link QuantileQuantileDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public QuantileQuantileDiagram ofQuantileQuantileDiagram() throws MetricParameterException
    {
        return (QuantileQuantileDiagram) new QuantileQuantileDiagram.QuantileQuantileDiagramBuilder().setOutputFactory( outputFactory )
                                                                                                     .build();
    }

    /**
     * Return a default {@link RootMeanSquareError} function.
     * 
     * @return a default {@link RootMeanSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public RootMeanSquareError ofRootMeanSquareError() throws MetricParameterException
    {
        return (RootMeanSquareError) new RootMeanSquareErrorBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link ReliabilityDiagram} function.
     * 
     * @return a default {@link ReliabilityDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ReliabilityDiagram ofReliabilityDiagram() throws MetricParameterException
    {
        return (ReliabilityDiagram) new ReliabilityDiagramBuilder().setOutputFactory( outputFactory )
                                                                   .build();
    }

    /**
     * Return a default {@link RelativeOperatingCharacteristicDiagram} function.
     * 
     * @return a default {@link RelativeOperatingCharacteristicDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public RelativeOperatingCharacteristicDiagram ofRelativeOperatingCharacteristic() throws MetricParameterException
    {
        return (RelativeOperatingCharacteristicDiagram) new RelativeOperatingCharacteristicBuilder().setOutputFactory( outputFactory )
                                                                                                    .build();
    }

    /**
     * Return a default {@link RelativeOperatingCharacteristicScore} function.
     * 
     * @return a default {@link RelativeOperatingCharacteristicScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public RelativeOperatingCharacteristicScore ofRelativeOperatingCharacteristicScore() throws MetricParameterException
    {
        return (RelativeOperatingCharacteristicScore) new RelativeOperatingCharacteristicScoreBuilder().setOutputFactory( outputFactory )
                                                                                                       .build();
    }

    /**
     * Return a default {@link ContinuousRankedProbabilityScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilityScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContinuousRankedProbabilityScore ofContinuousRankedProbabilityScore() throws MetricParameterException
    {
        return (ContinuousRankedProbabilityScore) new CRPSBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link ContinuousRankedProbabilitySkillScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilitySkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContinuousRankedProbabilitySkillScore ofContinuousRankedProbabilitySkillScore()
            throws MetricParameterException
    {
        return (ContinuousRankedProbabilitySkillScore) new CRPSSBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link IndexOfAgreement} function.
     * 
     * @return a default {@link IndexOfAgreement} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public IndexOfAgreement ofIndexOfAgreement() throws MetricParameterException
    {
        return (IndexOfAgreement) new IndexOfAgreementBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link KlingGuptaEfficiency} function.
     * 
     * @return a default {@link KlingGuptaEfficiency} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public KlingGuptaEfficiency ofKlingGuptaEfficiency() throws MetricParameterException
    {
        return (KlingGuptaEfficiency) new KlingGuptaEfficiencyBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link SampleSize} function.
     * 
     * @param <T> the type of {@link MetricInput}
     * @return a default {@link SampleSize} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public <T extends MetricInput<?>> SampleSize<T> ofSampleSize() throws MetricParameterException
    {
        return (SampleSize<T>) new SampleSizeBuilder<T>().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link RankHistogram} function.
     * 
     * @return a default {@link RankHistogram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public RankHistogram ofRankHistogram() throws MetricParameterException
    {
        return (RankHistogram) new RankHistogramBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link FrequencyBias} function.
     * 
     * @return a default {@link FrequencyBias} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public FrequencyBias ofFrequencyBias() throws MetricParameterException
    {
        return (FrequencyBias) new FrequencyBias.FrequencyBiasBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link BoxPlotErrorByObserved} function.
     * 
     * @return a default {@link BoxPlotErrorByObserved} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public BoxPlotErrorByObserved ofBoxPlotErrorByObserved() throws MetricParameterException
    {
        return (BoxPlotErrorByObserved) new BoxPlotErrorByObservedBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link BoxPlotErrorByForecast} function.
     * 
     * @return a default {@link BoxPlotErrorByForecast} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public BoxPlotErrorByForecast ofBoxPlotErrorByForecast() throws MetricParameterException
    {
        return (BoxPlotErrorByForecast) new BoxPlotErrorByForecastBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link VolumetricEfficiency} function.
     * 
     * @return a default {@link VolumetricEfficiency} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public VolumetricEfficiency ofVolumetricEfficiency() throws MetricParameterException
    {
        return (VolumetricEfficiency) new VolumetricEfficiencyBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link TimeToPeakError} function.
     * 
     * @return a default {@link TimeToPeakError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public TimeToPeakError ofTimeToPeakError() throws MetricParameterException
    {
        return (TimeToPeakError) new TimeToPeakErrorBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link TimeToPeakRelativeError} function.
     * 
     * @return a default {@link TimeToPeakRelativeError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public TimeToPeakRelativeError ofTimeToPeakRelativeError() throws MetricParameterException
    {
        return (TimeToPeakRelativeError) new TimeToPeakRelativeErrorBuilder().setOutputFactory( outputFactory ).build();
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

    public TimingErrorDurationStatistics ofTimingErrorDurationStatistics( MetricConstants identifier,
                                                                          Set<MetricConstants> statistics )
            throws MetricParameterException
    {
        return new TimingErrorDurationStatisticsBuilder().setStatistics( statistics )
                                                         .setID( identifier )
                                                         .setOutputFactory( outputFactory )
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
     * Helper that interprets the input configuration and returns a list of {@link MetricOutputGroup} whose results 
     * should be cached across successive calls to a {@link MetricProcessor}.
     * 
     * @param projectConfig the project configuration
     * @return a list of output types that should be cached
     * @throws MetricConfigException if the configuration is invalid
     * @throws NullPointerException if the input is null
     */

    private static Set<MetricOutputGroup> getCacheListFromProjectConfig( ProjectConfig projectConfig )
            throws MetricConfigException
    {
        // Always cache ordinary scores and paired output for timing error metrics 
        Set<MetricOutputGroup> returnMe = new TreeSet<>();
        returnMe.add( MetricOutputGroup.DOUBLE_SCORE );
        returnMe.add( MetricOutputGroup.PAIRED );

        // Cache other outputs as required
        MetricOutputGroup[] options = MetricOutputGroup.values();
        for ( MetricOutputGroup next : options )
        {
            if ( !returnMe.contains( next )
                 && MetricConfigHelper.hasTheseOutputsByThresholdLead( projectConfig, next ) )
            {
                returnMe.add( next );
            }
        }

        // Never cache box plot output, as it does not apply to thresholds
        returnMe.remove( MetricOutputGroup.BOXPLOT );

        // Never cache duration score output as timing error summary statistics are computed once all data 
        // is available
        returnMe.remove( MetricOutputGroup.DURATION_SCORE );

        return Collections.unmodifiableSet( returnMe );
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
     * @param dataFactory a {@link DataFactory}
     */

    private MetricFactory( final DataFactory dataFactory )
    {
        if ( Objects.isNull( dataFactory ) )
        {
            throw new IllegalArgumentException( "Specify a non-null metric output factory to construct the "
                                                + "metric factory." );
        }
        this.outputFactory = dataFactory;
    }

}
