package wres.engine.statistics.metric;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import wres.config.generated.MetricConfigName;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.Threshold;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.inputs.pairs.PairedInput;
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
import wres.engine.statistics.metric.config.MetricConfigurationException;
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
import wres.engine.statistics.metric.singlevalued.CoefficientOfDetermination;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons;
import wres.engine.statistics.metric.singlevalued.IndexOfAgreement;
import wres.engine.statistics.metric.singlevalued.IndexOfAgreement.IndexOfAgreementBuilder;
import wres.engine.statistics.metric.singlevalued.KlingGuptaEfficiency;
import wres.engine.statistics.metric.singlevalued.KlingGuptaEfficiency.KlingGuptaEfficiencyBuilder;
import wres.engine.statistics.metric.singlevalued.MeanAbsoluteError;
import wres.engine.statistics.metric.singlevalued.MeanError;
import wres.engine.statistics.metric.singlevalued.MeanSquareError;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;
import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareError;
import wres.engine.statistics.metric.singlevalued.VolumetricEfficiency;
import wres.engine.statistics.metric.singlevalued.VolumetricEfficiency.VolumetricEfficiencyBuilder;
import wres.engine.statistics.metric.timeseries.TimeToPeakError;
import wres.engine.statistics.metric.timeseries.TimeToPeakError.TimeToPeakErrorBuilder;
import wres.engine.statistics.metric.timeseries.TimeToPeakErrorStatistics;
import wres.engine.statistics.metric.timeseries.TimeToPeakErrorStatistics.TimeToPeakErrorStatisticBuilder;

/**
 * <p>
 * A factory class for constructing metrics.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.2
 * @since 0.1
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

    private static final String UNRECOGNIZED_METRIC_ERROR = "Unrecognized metric for identifier";

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

    private DataFactory outputFactory = null;

    /**
     * Cached {@link Metric} that consume {@link SingleValuedPairs} and produce {@link DoubleScoreOutput}. 
     */

    private Map<MetricConstants, Metric<SingleValuedPairs, DoubleScoreOutput>> singleValuedScore;

    /**
     * Cached {@link Metric} that consume {@link EnsemblePairs} and produce {@link DoubleScoreOutput}. 
     */

    private Map<MetricConstants, Metric<EnsemblePairs, DoubleScoreOutput>> ensembleScore;

    /**
     * Cached {@link Collectable} that consume {@link SingleValuedPairs} and produce {@link DoubleScoreOutput}. 
     */

    private Map<MetricConstants, Collectable<SingleValuedPairs, DoubleScoreOutput, DoubleScoreOutput>> singleValuedScoreCol;

    /**
     * Cached {@link Collectable} that consume {@link DichotomousPairs} and produce {@link DoubleScoreOutput}. 
     */

    private Map<MetricConstants, Collectable<DichotomousPairs, MatrixOutput, DoubleScoreOutput>> dichotomousScoreCol;

    /**
     * Cached {@link Metric} that consume {@link DiscreteProbabilityPairs} and produce {@link DoubleScoreOutput}. 
     */

    private Map<MetricConstants, Metric<DiscreteProbabilityPairs, DoubleScoreOutput>> discreteProbabilityScore;

    /**
     * Cached {@link Metric} that consume {@link DiscreteProbabilityPairs} and produce {@link MultiVectorOutput}. 
     */

    private Map<MetricConstants, Metric<DiscreteProbabilityPairs, MultiVectorOutput>> discreteProbabilityMultiVector;

    /**
     * Cached {@link Metric} that consume {@link EnsemblePairs} and produce {@link BoxPlotOutput}. 
     */

    private Map<MetricConstants, Metric<EnsemblePairs, BoxPlotOutput>> ensembleBoxPlot;

    /**
     * Cached {@link Metric} that consume {@link TimeSeriesOfSingleValuedPairs} and produce {@link PairedOutput}. 
     */

    private Map<MetricConstants, Metric<TimeSeriesOfSingleValuedPairs, PairedOutput<Instant, Duration>>> singleValuedTimeSeries;

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
     * @param externalThresholds an optional set of external thresholds (one per metric), may be null
     * @param thresholdExecutor an executor service for processing thresholds
     * @param metricExecutor an executor service for processing metrics
     * @return a metric processor
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorForProject getMetricProcessorForProject( final ProjectConfig projectConfig,
                                                                   final Map<MetricConfigName, Set<Threshold>> externalThresholds,
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
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final MetricOutputGroup... mergeList )
                    throws MetricProcessorException
    {
        return ofMetricProcessorByTimeSingleValuedPairs( config,
                                                         null,
                                                         ForkJoinPool.commonPool(),
                                                         ForkJoinPool.commonPool(),
                                                         mergeList );
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
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs>
            ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                  final MetricOutputGroup... mergeList )
                    throws MetricProcessorException
    {
        return ofMetricProcessorByTimeEnsemblePairs( config,
                                                     null,
                                                     ForkJoinPool.commonPool(),
                                                     ForkJoinPool.commonPool(),
                                                     mergeList );
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
     * @param externalThresholds an optional set of external thresholds (one per metric), may be null
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final Map<MetricConfigName, Set<Threshold>> externalThresholds,
                                                      final MetricOutputGroup... mergeList )
                    throws MetricProcessorException
    {
        return ofMetricProcessorByTimeSingleValuedPairs( config,
                                                         externalThresholds,
                                                         ForkJoinPool.commonPool(),
                                                         ForkJoinPool.commonPool(),
                                                         mergeList );
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
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs>
            ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                  final Map<MetricConfigName, Set<Threshold>> externalThresholds,
                                                  final MetricOutputGroup... mergeList )
                    throws MetricProcessorException
    {
        return ofMetricProcessorByTimeEnsemblePairs( config,
                                                     externalThresholds,
                                                     ForkJoinPool.commonPool(),
                                                     ForkJoinPool.commonPool(),
                                                     mergeList );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link SingleValuedPairs}. Uses the input 
     * project configuration to determine which results should be merged and cached across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param externalThresholds an optional set of external thresholds (one per metric), may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final Map<MetricConfigName, Set<Threshold>> externalThresholds,
                                                      final ExecutorService thresholdExecutor,
                                                      final ExecutorService metricExecutor )
                    throws MetricProcessorException
    {
        try
        {
            return ofMetricProcessorByTimeSingleValuedPairs( config,
                                                             externalThresholds,
                                                             thresholdExecutor,
                                                             metricExecutor,
                                                             getCacheListFromProjectConfig( config ) );
        }
        catch ( MetricConfigurationException e )
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
     * @param externalThresholds an optional set of external thresholds (one per metric), may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs> ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                                                      final Map<MetricConfigName, Set<Threshold>> externalThresholds,
                                                                                      final ExecutorService thresholdExecutor,
                                                                                      final ExecutorService metricExecutor )
            throws MetricProcessorException
    {
        try
        {
            return ofMetricProcessorByTimeEnsemblePairs( config,
                                                         externalThresholds,
                                                         thresholdExecutor,
                                                         metricExecutor,
                                                         getCacheListFromProjectConfig( config ) );
        }
        catch ( MetricConfigurationException e )
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
     * @param externalThresholds an optional set of external thresholds (one per metric), may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final Map<MetricConfigName, Set<Threshold>> externalThresholds,
                                                      final ExecutorService thresholdExecutor,
                                                      final ExecutorService metricExecutor,
                                                      final MetricOutputGroup... mergeList )
                    throws MetricProcessorException
    {
        try
        {
            return new MetricProcessorByTimeSingleValuedPairs( outputFactory,
                                                               config,
                                                               externalThresholds,
                                                               thresholdExecutor,
                                                               metricExecutor,
                                                               mergeList );
        }
        catch ( MetricConfigurationException e )
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
     * @param externalThresholds an optional set of external thresholds (one per metric), may be null
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs> ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                                                      final Map<MetricConfigName, Set<Threshold>> externalThresholds,
                                                                                      final ExecutorService thresholdExecutor,
                                                                                      final ExecutorService metricExecutor,
                                                                                      final MetricOutputGroup... mergeList )
            throws MetricProcessorException
    {
        try
        {
            return new MetricProcessorByTimeEnsemblePairs( outputFactory,
                                                           config,
                                                           externalThresholds,
                                                           thresholdExecutor,
                                                           metricExecutor,
                                                           mergeList );
        }
        catch ( MetricConfigurationException e )
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
        // Build the store if required
        buildSingleValuedScoreStore();
        for ( MetricConstants next : metric )
        {
            if ( singleValuedScoreCol.containsKey( next ) )
            {
                builder.add( singleValuedScoreCol.get( next ) );
            }
            else if ( singleValuedScore.containsKey( next ) )
            {
                builder.add( singleValuedScore.get( next ) );
            }
            else
            {
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
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
            builder.add( ofSingleValuedMultiVector( next ) );
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
        // Build the store if required
        buildDiscreteProbabilityScoreStore();
        for ( MetricConstants next : metric )
        {
            if ( !discreteProbabilityScore.containsKey( next ) )
            {
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
            }
            builder.add( discreteProbabilityScore.get( next ) );
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
        // Build store if required
        buildDichotomousScalarStore();
        final MetricCollectionBuilder<DichotomousPairs, MatrixOutput, DoubleScoreOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            if ( !dichotomousScoreCol.containsKey( next ) )
            {
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
            }
            builder.add( dichotomousScoreCol.get( next ) );
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
        // Build the store if required
        buildDiscreteProbabilityMultiVectorStore();
        for ( MetricConstants next : metric )
        {
            if ( !discreteProbabilityMultiVector.containsKey( next ) )
            {
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
            }
            builder.add( discreteProbabilityMultiVector.get( next ) );
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
            builder.add( ofDichotomousMatrix( next ) );
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
        // Build the store if required
        buildEnsembleScoreStore();
        for ( MetricConstants next : metric )
        {
            if ( !ensembleScore.containsKey( next ) )
            {
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
            }
            builder.add( ensembleScore.get( next ) );
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
            builder.add( ofEnsembleMultiVector( next ) );
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
        // Build store if required
        buildEnsembleBoxPlotStore();
        final MetricCollectionBuilder<EnsemblePairs, BoxPlotOutput, BoxPlotOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            if ( !ensembleBoxPlot.containsKey( next ) )
            {
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
            }
            builder.add( ensembleBoxPlot.get( next ) );
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
        // Build store if required
        buildSingleValuedScoreStore();
        if ( singleValuedScoreCol.containsKey( metric ) )
        {
            return singleValuedScoreCol.get( metric );
        }
        else if ( singleValuedScore.containsKey( metric ) )
        {
            return singleValuedScore.get( metric );
        }
        else
        {
            throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
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
        // Build the store if required
        buildSingleValuedTimeSeriesStore();
        for ( MetricConstants next : metric )
        {
            if ( !singleValuedTimeSeries.containsKey( next ) )
            {
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
            }
            builder.add( singleValuedTimeSeries.get( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
        return builder.build();
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
        // Build store if required
        buildDiscreteProbabilityScoreStore();
        if ( discreteProbabilityScore.containsKey( metric ) )
        {
            return discreteProbabilityScore.get( metric );
        }
        else
        {
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

    public Metric<DichotomousPairs, DoubleScoreOutput> ofDichotomousScore( MetricConstants metric )
            throws MetricParameterException
    {
        // Build store if required
        buildDichotomousScalarStore();
        if ( dichotomousScoreCol.containsKey( metric ) )
        {
            return dichotomousScoreCol.get( metric );
        }
        else
        {
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
        // Build store if required
        buildDiscreteProbabilityMultiVectorStore();
        if ( discreteProbabilityMultiVector.containsKey( metric ) )
        {
            return discreteProbabilityMultiVector.get( metric );
        }
        else
        {
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
        // Build store if required
        buildEnsembleScoreStore();
        if ( ensembleScore.containsKey( metric ) )
        {
            return ensembleScore.get( metric );
        }
        else
        {
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
        // Build store if required
        buildEnsembleBoxPlotStore();
        if ( ensembleBoxPlot.containsKey( metric ) )
        {
            return ensembleBoxPlot.get( metric );
        }
        else
        {
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
     * Return a default {@link BiasFraction} function.
     * 
     * @return a default {@link BiasFraction} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BiasFraction ofBiasFraction() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (BiasFraction) singleValuedScore.get( MetricConstants.BIAS_FRACTION );
    }

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BrierScore ofBrierScore() throws MetricParameterException
    {
        buildDiscreteProbabilityScoreStore();
        return (BrierScore) discreteProbabilityScore.get( MetricConstants.BRIER_SCORE );
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BrierSkillScore ofBrierSkillScore() throws MetricParameterException
    {
        buildDiscreteProbabilityScoreStore();
        return (BrierSkillScore) discreteProbabilityScore.get( MetricConstants.BRIER_SKILL_SCORE );
    }

    /**
     * Return a default {@link CoefficientOfDetermination} function.
     * 
     * @return a default {@link CoefficientOfDetermination} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public CorrelationPearsons ofCoefficientOfDetermination() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (CoefficientOfDetermination) singleValuedScoreCol.get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
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
        buildSingleValuedScoreStore();
        return (CorrelationPearsons) singleValuedScoreCol.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
    }

    /**
     * Return a default {@link ThreatScore} function.
     * 
     * @return a default {@link ThreatScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public ThreatScore ofCriticalSuccessIndex() throws MetricParameterException
    {
        buildDichotomousScalarStore();
        return (ThreatScore) dichotomousScoreCol.get( MetricConstants.THREAT_SCORE );
    }

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public EquitableThreatScore ofEquitableThreatScore() throws MetricParameterException
    {
        buildDichotomousScalarStore();
        return (EquitableThreatScore) dichotomousScoreCol.get( MetricConstants.EQUITABLE_THREAT_SCORE );
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public MeanAbsoluteError ofMeanAbsoluteError() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (MeanAbsoluteError) singleValuedScore.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public MeanError ofMeanError() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (MeanError) singleValuedScore.get( MetricConstants.MEAN_ERROR );
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public MeanSquareError<SingleValuedPairs> ofMeanSquareError() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (MeanSquareError<SingleValuedPairs>) singleValuedScoreCol.get( MetricConstants.MEAN_SQUARE_ERROR );
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public MeanSquareErrorSkillScore<SingleValuedPairs> ofMeanSquareErrorSkillScore() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (MeanSquareErrorSkillScore<SingleValuedPairs>) singleValuedScore.get( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE );
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a dichotomous event.
     * 
     * @return a default {@link PeirceSkillScore} function for a dichotomous event
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public PeirceSkillScore<DichotomousPairs> ofPeirceSkillScore() throws MetricParameterException
    {
        buildDichotomousScalarStore();
        return (PeirceSkillScore<DichotomousPairs>) dichotomousScoreCol.get( MetricConstants.PEIRCE_SKILL_SCORE );
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
        buildDichotomousScalarStore();
        return (ProbabilityOfDetection) dichotomousScoreCol.get( MetricConstants.PROBABILITY_OF_DETECTION );
    }

    /**
     * Return a default {@link ProbabilityOfFalseDetection} function.
     * 
     * @return a default {@link ProbabilityOfFalseDetection} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ProbabilityOfFalseDetection ofProbabilityOfFalseDetection() throws MetricParameterException
    {
        buildDichotomousScalarStore();
        return (ProbabilityOfFalseDetection) dichotomousScoreCol.get( MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
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
        buildSingleValuedScoreStore();
        return (RootMeanSquareError) singleValuedScoreCol.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
    }

    /**
     * Return a default {@link ReliabilityDiagram} function.
     * 
     * @return a default {@link ReliabilityDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ReliabilityDiagram ofReliabilityDiagram() throws MetricParameterException
    {
        buildDiscreteProbabilityMultiVectorStore();
        return (ReliabilityDiagram) discreteProbabilityMultiVector.get( MetricConstants.RELIABILITY_DIAGRAM );
    }

    /**
     * Return a default {@link RelativeOperatingCharacteristicDiagram} function.
     * 
     * @return a default {@link RelativeOperatingCharacteristicDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public RelativeOperatingCharacteristicDiagram ofRelativeOperatingCharacteristic() throws MetricParameterException
    {
        buildDiscreteProbabilityMultiVectorStore();
        return (RelativeOperatingCharacteristicDiagram) discreteProbabilityMultiVector.get( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM );
    }

    /**
     * Return a default {@link RelativeOperatingCharacteristicScore} function.
     * 
     * @return a default {@link RelativeOperatingCharacteristicScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public RelativeOperatingCharacteristicScore ofRelativeOperatingCharacteristicScore() throws MetricParameterException
    {
        buildDiscreteProbabilityScoreStore();
        return (RelativeOperatingCharacteristicScore) discreteProbabilityScore.get( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE );
    }

    /**
     * Return a default {@link ContinuousRankedProbabilityScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilityScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContinuousRankedProbabilityScore ofContinuousRankedProbabilityScore() throws MetricParameterException
    {
        buildEnsembleScoreStore();
        return (ContinuousRankedProbabilityScore) ensembleScore.get( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE );
    }

    /**
     * Return a default {@link ContinuousRankedProbabilitySkillScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilitySkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContinuousRankedProbabilityScore ofContinuousRankedProbabilitySkillScore() throws MetricParameterException
    {
        buildEnsembleScoreStore();
        return (ContinuousRankedProbabilitySkillScore) ensembleScore.get( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
    }

    /**
     * Return a default {@link IndexOfAgreement} function.
     * 
     * @return a default {@link IndexOfAgreement} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public IndexOfAgreement ofIndexOfAgreement() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (IndexOfAgreement) singleValuedScore.get( MetricConstants.INDEX_OF_AGREEMENT );
    }

    /**
     * Return a default {@link KlingGuptaEfficiency} function.
     * 
     * @return a default {@link KlingGuptaEfficiency} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public KlingGuptaEfficiency ofKlingGuptaEfficiency() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (KlingGuptaEfficiency) singleValuedScore.get( MetricConstants.KLING_GUPTA_EFFICIENCY );
    }

    /**
     * Return a default {@link SampleSize} function.
     * 
     * @param <T> the type of {@link MetricInput}
     * @return a default {@link SampleSize} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    <T extends PairedInput<?>> SampleSize<T> ofSampleSize() throws MetricParameterException
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
        buildDichotomousScalarStore();
        return (FrequencyBias) dichotomousScoreCol.get( MetricConstants.FREQUENCY_BIAS );
    }

    /**
     * Return a default {@link BoxPlotErrorByObserved} function.
     * 
     * @return a default {@link BoxPlotErrorByObserved} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public BoxPlotErrorByObserved ofBoxPlotErrorByObserved() throws MetricParameterException
    {
        buildEnsembleBoxPlotStore();
        return (BoxPlotErrorByObserved) ensembleBoxPlot.get( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE );
    }

    /**
     * Return a default {@link BoxPlotErrorByForecast} function.
     * 
     * @return a default {@link BoxPlotErrorByForecast} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public BoxPlotErrorByForecast ofBoxPlotErrorByForecast() throws MetricParameterException
    {
        buildEnsembleBoxPlotStore();
        return (BoxPlotErrorByForecast) ensembleBoxPlot.get( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE );
    }

    /**
     * Return a default {@link VolumetricEfficiency} function.
     * 
     * @return a default {@link VolumetricEfficiency} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public VolumetricEfficiency ofVolumetricEfficiency() throws MetricParameterException
    {
        buildSingleValuedScoreStore();
        return (VolumetricEfficiency) singleValuedScore.get( MetricConstants.VOLUMETRIC_EFFICIENCY );
    }

    /**
     * Return a default {@link TimeToPeakError} function.
     * 
     * @return a default {@link TimeToPeakError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public TimeToPeakError ofTimeToPeakError() throws MetricParameterException
    {
        buildSingleValuedTimeSeriesStore();
        return (TimeToPeakError) singleValuedTimeSeries.get( MetricConstants.TIME_TO_PEAK_ERROR );
    }

    /**
     * Return a default {@link TimeToPeakErrorStatistics} function for a prescribed set of {@link MetricConstants}.
     * For each of the {@link MetricConstants}, the {@link MetricConstants#isInGroup(ScoreOutputGroup)} should return 
     * <code>true</code> when supplied with {@link ScoreOutputGroup#UNIVARIATE_STATISTIC}.
     * 
     * @param statistics the identifiers for summary statistics
     * @return a default {@link TimeToPeakErrorStatistics} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public TimeToPeakErrorStatistics ofTimeToPeakErrorStatistics( MetricConstants... statistics )
            throws MetricParameterException
    {
        return (TimeToPeakErrorStatistics) new TimeToPeakErrorStatisticBuilder().setStatistic( statistics )
                                                                                .setOutputFactory( outputFactory )
                                                                                .build();
    }

    /**
     * Helper that interprets the input configuration and returns a list of {@link MetricOutputGroup} whose results 
     * should be cached across successive calls to a {@link MetricProcessor}.
     * 
     * @param projectConfig the project configuration
     * @return a list of output types that should be cached
     * @throws MetricConfigurationException if the configuration is invalid
     * @throws NullPointerException if the input is null
     */

    private MetricOutputGroup[] getCacheListFromProjectConfig( ProjectConfig projectConfig )
            throws MetricConfigurationException
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

        return returnMe.toArray( new MetricOutputGroup[returnMe.size()] );
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

    /**
     * Builds the store of metrics that consume {@link SingleValuedPairs} and produce {@link DoubleScoreOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildSingleValuedScoreStore() throws MetricParameterException
    {
        if ( Objects.isNull( singleValuedScore ) )
        {
            // Ordinary metrics
            singleValuedScore = new EnumMap<>( MetricConstants.class );
            singleValuedScore.put( MetricConstants.BIAS_FRACTION,
                                   new BiasFraction.BiasFractionBuilder().setOutputFactory( outputFactory ).build() );
            singleValuedScore.put( MetricConstants.MEAN_ABSOLUTE_ERROR,
                                   new MeanAbsoluteError.MeanAbsoluteErrorBuilder().setOutputFactory( outputFactory )
                                                                                   .build() );
            singleValuedScore.put( MetricConstants.MEAN_ERROR,
                                   new MeanError.MeanErrorBuilder().setOutputFactory( outputFactory )
                                                                   .build() );
            singleValuedScore.put( MetricConstants.SAMPLE_SIZE,
                                   new SampleSizeBuilder<SingleValuedPairs>().setOutputFactory( outputFactory )
                                                                             .build() );
            singleValuedScore.put( MetricConstants.INDEX_OF_AGREEMENT,
                                   new IndexOfAgreementBuilder().setOutputFactory( outputFactory ).build() );
            singleValuedScore.put( MetricConstants.VOLUMETRIC_EFFICIENCY,
                                   new VolumetricEfficiencyBuilder().setOutputFactory( outputFactory ).build() );
            singleValuedScore.put( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                   new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>().setOutputFactory( outputFactory )
                                                                                                     .build() );
            singleValuedScore.put( MetricConstants.KLING_GUPTA_EFFICIENCY,
                                   new KlingGuptaEfficiencyBuilder().setOutputFactory( outputFactory ).build() );
            // Collectable metrics
            singleValuedScoreCol = new EnumMap<>( MetricConstants.class );
            singleValuedScoreCol.put( MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                      (CoefficientOfDetermination) new CoefficientOfDetermination.CoefficientOfDeterminationBuilder().setOutputFactory( outputFactory )
                                                                                                                                     .build() );
            singleValuedScoreCol.put( MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                      (CorrelationPearsons) new CorrelationPearsons.CorrelationPearsonsBuilder().setOutputFactory( outputFactory )
                                                                                                                .build() );
            singleValuedScoreCol.put( MetricConstants.MEAN_SQUARE_ERROR,
                                      (MeanSquareError<SingleValuedPairs>) new MeanSquareError.MeanSquareErrorBuilder<>().setOutputFactory( outputFactory )
                                                                                                                         .build() );
            // RMSE depends on MSE, so add afterwards
            singleValuedScoreCol.put( MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                                      (RootMeanSquareError) new RootMeanSquareError.RootMeanSquareErrorBuilder().setOutputFactory( outputFactory )
                                                                                                                .build() );
        }
    }

    /**
     * Builds the store of metrics that consume {@link DichotomousPairs} and produce {@link DoubleScoreOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildDichotomousScalarStore() throws MetricParameterException
    {
        if ( Objects.isNull( dichotomousScoreCol ) )
        {
            dichotomousScoreCol = new EnumMap<>( MetricConstants.class );
            dichotomousScoreCol.put( MetricConstants.THREAT_SCORE,
                                     (ThreatScore) new ThreatScore.CriticalSuccessIndexBuilder().setOutputFactory( outputFactory )
                                                                                                .build() );
            dichotomousScoreCol.put( MetricConstants.EQUITABLE_THREAT_SCORE,
                                     (EquitableThreatScore) new EquitableThreatScore.EquitableThreatScoreBuilder().setOutputFactory( outputFactory )
                                                                                                                  .build() );
            dichotomousScoreCol.put( MetricConstants.PEIRCE_SKILL_SCORE,
                                     (PeirceSkillScore<DichotomousPairs>) new PeirceSkillScore.PeirceSkillScoreBuilder<DichotomousPairs>().setOutputFactory( outputFactory )
                                                                                                                                          .build() );
            dichotomousScoreCol.put( MetricConstants.PROBABILITY_OF_DETECTION,
                                     (ProbabilityOfDetection) new ProbabilityOfDetection.ProbabilityOfDetectionBuilder().setOutputFactory( outputFactory )
                                                                                                                        .build() );
            dichotomousScoreCol.put( MetricConstants.PROBABILITY_OF_FALSE_DETECTION,
                                     (ProbabilityOfFalseDetection) new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder().setOutputFactory( outputFactory )
                                                                                                                                       .build() );
            dichotomousScoreCol.put( MetricConstants.FREQUENCY_BIAS,
                                     (FrequencyBias) new FrequencyBias.FrequencyBiasBuilder().setOutputFactory( outputFactory )
                                                                                             .build() );
        }
    }

    /**
     * Builds the store of metrics that consume {@link EnsemblePairs} and produce {@link DoubleScoreOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildEnsembleScoreStore() throws MetricParameterException
    {
        if ( Objects.isNull( ensembleScore ) )
        {
            ensembleScore = new EnumMap<>( MetricConstants.class );
            ensembleScore.put( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                               new CRPSBuilder().setOutputFactory( outputFactory ).build() );
            ensembleScore.put( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                               new CRPSSBuilder().setOutputFactory( outputFactory ).build() );
            ensembleScore.put( MetricConstants.SAMPLE_SIZE,
                               new SampleSizeBuilder<EnsemblePairs>().setOutputFactory( outputFactory ).build() );

        }
    }

    /**
     * Builds the store of metrics that consume {@link DiscreteProbabilityPairs} and produce 
     * {@link DoubleScoreOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildDiscreteProbabilityScoreStore() throws MetricParameterException
    {
        if ( Objects.isNull( discreteProbabilityScore ) )
        {
            discreteProbabilityScore = new EnumMap<>( MetricConstants.class );
            discreteProbabilityScore.put( MetricConstants.BRIER_SCORE,
                                          new BrierScore.BrierScoreBuilder().setOutputFactory( outputFactory )
                                                                            .build() );
            discreteProbabilityScore.put( MetricConstants.BRIER_SKILL_SCORE,
                                          new BrierSkillScore.BrierSkillScoreBuilder().setOutputFactory( outputFactory )
                                                                                      .build() );
            discreteProbabilityScore.put( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE,
                                          new RelativeOperatingCharacteristicScoreBuilder().setOutputFactory( outputFactory )
                                                                                           .build() );
        }
    }

    /**
     * Builds the store of metrics that consume {@link DiscreteProbabilityPairs} and produce 
     * {@link MultiVectorOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildDiscreteProbabilityMultiVectorStore() throws MetricParameterException
    {
        if ( Objects.isNull( discreteProbabilityMultiVector ) )
        {
            discreteProbabilityMultiVector = new EnumMap<>( MetricConstants.class );
            discreteProbabilityMultiVector.put( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                new RelativeOperatingCharacteristicBuilder().setOutputFactory( outputFactory )
                                                                                            .build() );
            discreteProbabilityMultiVector.put( MetricConstants.RELIABILITY_DIAGRAM,
                                                new ReliabilityDiagramBuilder().setOutputFactory( outputFactory )
                                                                               .build() );
        }
    }

    /**
     * Builds the store of metrics that consume {@link EnsemblePairs} and produce 
     * {@link BoxPlotOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildEnsembleBoxPlotStore() throws MetricParameterException
    {
        if ( Objects.isNull( ensembleBoxPlot ) )
        {
            ensembleBoxPlot = new EnumMap<>( MetricConstants.class );
            ensembleBoxPlot.put( MetricConstants.BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE,
                                 new BoxPlotErrorByObservedBuilder().setOutputFactory( outputFactory ).build() );
            ensembleBoxPlot.put( MetricConstants.BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE,
                                 new BoxPlotErrorByForecastBuilder().setOutputFactory( outputFactory ).build() );
        }
    }

    /**
     * Builds the store of metrics that consumes {@link TimeSeriesOfSingleValuedPairs} and produces 
     * {@link PairedOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildSingleValuedTimeSeriesStore() throws MetricParameterException
    {
        if ( Objects.isNull( singleValuedTimeSeries ) )
        {
            singleValuedTimeSeries = new EnumMap<>( MetricConstants.class );
            singleValuedTimeSeries.put( MetricConstants.TIME_TO_PEAK_ERROR,
                                        new TimeToPeakErrorBuilder().setOutputFactory( outputFactory ).build() );
        }
    }

}
