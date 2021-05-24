package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConfigException;
import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.Ensemble;
import wres.datamodel.pools.Pool;
import wres.datamodel.Probability;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.Metrics;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
import wres.datamodel.metrics.MetricConstants.StatisticType;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.thresholds.ThresholdsByMetric;
import wres.datamodel.thresholds.ThresholdsGenerator;
import wres.engine.statistics.metric.MetricCollection.Builder;
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
import wres.engine.statistics.metric.discreteprobability.RelativeOperatingCharacteristicScore;
import wres.engine.statistics.metric.discreteprobability.ReliabilityDiagram;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByForecast;
import wres.engine.statistics.metric.ensemble.BoxPlotErrorByObserved;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilityScore;
import wres.engine.statistics.metric.ensemble.ContinuousRankedProbabilitySkillScore;
import wres.engine.statistics.metric.ensemble.EnsembleQuantileQuantileDiagram;
import wres.engine.statistics.metric.ensemble.RankHistogram;
import wres.engine.statistics.metric.processing.MetricProcessor;
import wres.engine.statistics.metric.processing.MetricProcessorByTime;
import wres.engine.statistics.metric.processing.MetricProcessorByTimeEnsemblePairs;
import wres.engine.statistics.metric.processing.MetricProcessorByTimeSingleValuedPairs;
import wres.engine.statistics.metric.singlevalued.BiasFraction;
import wres.engine.statistics.metric.singlevalued.BoxPlotError;
import wres.engine.statistics.metric.singlevalued.BoxPlotPercentageError;
import wres.engine.statistics.metric.singlevalued.CoefficientOfDetermination;
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons;
import wres.engine.statistics.metric.singlevalued.IndexOfAgreement;
import wres.engine.statistics.metric.singlevalued.KlingGuptaEfficiency;
import wres.engine.statistics.metric.singlevalued.MeanAbsoluteError;
import wres.engine.statistics.metric.singlevalued.MeanError;
import wres.engine.statistics.metric.singlevalued.MeanSquareError;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScore;
import wres.engine.statistics.metric.singlevalued.MeanSquareErrorSkillScoreNormalized;
import wres.engine.statistics.metric.singlevalued.MedianError;
import wres.engine.statistics.metric.singlevalued.QuantileQuantileDiagram;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareError;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareErrorNormalized;
import wres.engine.statistics.metric.singlevalued.SumOfSquareError;
import wres.engine.statistics.metric.singlevalued.VolumetricEfficiency;
import wres.engine.statistics.metric.singlevalued.univariate.Maximum;
import wres.engine.statistics.metric.singlevalued.univariate.Mean;
import wres.engine.statistics.metric.singlevalued.univariate.Minimum;
import wres.engine.statistics.metric.singlevalued.univariate.StandardDeviation;
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
     * Test seed system property name.
     */

    private static final String TEST_SEED_PROPERTY = "wres.systemTestSeed";

    /**
     * <p>Returns an instance of a {@link MetricProcessor} for processing single-valued pairs. Optionally, retain 
     * and merge the results associated with specific {@link StatisticType} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param config the project configuration
     * @param mergeSet an optional list of {@link StatisticType} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessor<Pool<Pair<Double, Double>>>
            ofMetricProcessorForSingleValuedPairs( final ProjectConfig config,
                                                   final Set<StatisticType> mergeSet )
    {
        ThresholdsByMetric thresholdsByMetric = ThresholdsGenerator.getThresholdsFromConfig( config );
        Metrics metrics = Metrics.of( thresholdsByMetric, 0 );

        return MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                    metrics,
                                                                    ForkJoinPool.commonPool(),
                                                                    ForkJoinPool.commonPool(),
                                                                    mergeSet );
    }

    /**
     * <p>Returns an instance of a {@link MetricProcessor} for processing ensemble pairs. Optionally, retain 
     * and merge the results associated with specific {@link StatisticType} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param config the project configuration
     * @param mergeSet an optional list of {@link StatisticType} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessor<Pool<Pair<Double, Ensemble>>>
            ofMetricProcessorForEnsemblePairs( final ProjectConfig config,
                                               final Set<StatisticType> mergeSet )
    {
        ThresholdsByMetric thresholdsByMetric = ThresholdsGenerator.getThresholdsFromConfig( config );
        Metrics metrics = Metrics.of( thresholdsByMetric, 0 );

        return MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                metrics,
                                                                ForkJoinPool.commonPool(),
                                                                ForkJoinPool.commonPool(),
                                                                mergeSet );
    }

    /**
     * <p>Returns an instance of a {@link MetricProcessor} for processing single-valued pairs. Optionally, retain 
     * and merge the results associated with specific {@link StatisticType} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param mergeSet an optional list of {@link StatisticType} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessor<Pool<Pair<Double, Double>>>
            ofMetricProcessorForSingleValuedPairs( final ProjectConfig config,
                                                   final Metrics metrics,
                                                   final Set<StatisticType> mergeSet )
    {
        return MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                    metrics,
                                                                    ForkJoinPool.commonPool(),
                                                                    ForkJoinPool.commonPool(),
                                                                    mergeSet );
    }

    /**
     * <p>Returns an instance of a {@link MetricProcessor} for processing ensemble pairs. Optionally, retain 
     * and merge the results associated with specific {@link StatisticType} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param mergeSet an optional list of {@link StatisticType} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessor<Pool<Pair<Double, Ensemble>>>
            ofMetricProcessorForEnsemblePairs( final ProjectConfig config,
                                               final Metrics metrics,
                                               final Set<StatisticType> mergeSet )
    {
        return MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                metrics,
                                                                ForkJoinPool.commonPool(),
                                                                ForkJoinPool.commonPool(),
                                                                mergeSet );
    }


    /**
     * Returns an instance of a {@link MetricProcessor} for processing single-valued pairs. Inspects the project
     * declaration for any statistics that should be retained and merged across successive calls to the processor
     * and creates a processor that retains and merges those statistics.
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessor<Pool<Pair<Double, Double>>>
            ofMetricProcessorForSingleValuedPairs( final ProjectConfig config,
                                                   final Metrics metrics,
                                                   final ExecutorService thresholdExecutor,
                                                   final ExecutorService metricExecutor )
    {
        Set<StatisticType> mergeSet = MetricConfigHelper.getCacheListFromProjectConfig( config );

        return MetricFactory.ofMetricProcessorForSingleValuedPairs( config,
                                                                    metrics,
                                                                    thresholdExecutor,
                                                                    metricExecutor,
                                                                    mergeSet );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing single-valued pairs. Inspects the project
     * declaration for any statistics that should be retained and merged across successive calls to the processor
     * and creates a processor that retains and merges those statistics.
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessor<Pool<Pair<Double, Ensemble>>>
            ofMetricProcessorForEnsemblePairs( final ProjectConfig config,
                                               final Metrics metrics,
                                               final ExecutorService thresholdExecutor,
                                               final ExecutorService metricExecutor )
    {
        Set<StatisticType> mergeSet = MetricConfigHelper.getCacheListFromProjectConfig( config );

        return MetricFactory.ofMetricProcessorForEnsemblePairs( config,
                                                                metrics,
                                                                thresholdExecutor,
                                                                metricExecutor,
                                                                mergeSet );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing single-valued pairs. Optionally, retain 
     * and merge the results associated with specific {@link StatisticType} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeSet an optional list of {@link StatisticType} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessor<Pool<Pair<Double, Double>>>
            ofMetricProcessorForSingleValuedPairs( final ProjectConfig config,
                                                   final Metrics metrics,
                                                   final ExecutorService thresholdExecutor,
                                                   final ExecutorService metricExecutor,
                                                   final Set<StatisticType> mergeSet )
    {
        return new MetricProcessorByTimeSingleValuedPairs( config,
                                                           metrics,
                                                           thresholdExecutor,
                                                           metricExecutor,
                                                           mergeSet );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing ensemble pairs. Optionally, retain 
     * and merge the results associated with specific {@link StatisticType} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param metrics the metrics to process
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeSet an optional set of {@link StatisticType} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigException if the metrics are configured incorrectly
     * @throws MetricParameterException if one or more metric parameters is set incorrectly
     */

    public static MetricProcessorByTime<Pool<Pair<Double, Ensemble>>>
            ofMetricProcessorForEnsemblePairs( final ProjectConfig config,
                                               final Metrics metrics,
                                               final ExecutorService thresholdExecutor,
                                               final ExecutorService metricExecutor,
                                               final Set<StatisticType> mergeSet )
    {
        return new MetricProcessorByTimeEnsemblePairs( config,
                                                       metrics,
                                                       thresholdExecutor,
                                                       metricExecutor,
                                                       mergeSet );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume single-valued pairs and produce
     * {@link DoubleScoreStatisticOuter}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ofSingleValuedScoreCollection( MetricConstants... metric )
    {
        return MetricFactory.ofSingleValuedScoreCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume single-valued pairs and produce
     * {@link DiagramStatisticOuter}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<Pool<Pair<Double, Double>>, DiagramStatisticOuter, DiagramStatisticOuter>
            ofSingleValuedDiagramCollection( MetricConstants... metric )
    {
        return MetricFactory.ofSingleValuedDiagramCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume discrete probability pairs and produce
     * {@link DoubleScoreStatisticOuter}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ofDiscreteProbabilityScoreCollection( MetricConstants... metric )
    {
        return MetricFactory.ofDiscreteProbabilityScoreCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume dichotomous pairs and produce
     * {@link DoubleScoreStatisticOuter}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ofDichotomousScoreCollection( MetricConstants... metric )
    {
        return MetricFactory.ofDichotomousScoreCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume discrete probability pairs and produce
     * {@link DiagramStatisticOuter}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter, DiagramStatisticOuter>
            ofDiscreteProbabilityDiagramCollection( MetricConstants... metric )
    {
        return MetricFactory.ofDiscreteProbabilityDiagramCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume ensemble pairs and produce
     * {@link DoubleScoreStatisticOuter}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static
            MetricCollection<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ofEnsembleScoreCollection( MetricConstants... metric )
    {
        return MetricFactory.ofEnsembleScoreCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume ensemble pairs and produce 
     * {@link DiagramStatisticOuter}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<Pool<Pair<Double, Ensemble>>, DiagramStatisticOuter, DiagramStatisticOuter>
            ofEnsembleDiagramCollection( MetricConstants... metric )
    {
        return MetricFactory.ofEnsembleDiagramCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume ensemble pairs and produce
     * {@link BoxplotStatisticOuter}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<Pool<Pair<Double, Ensemble>>, BoxplotStatisticOuter, BoxplotStatisticOuter>
            ofEnsembleBoxPlotCollection( MetricConstants... metric )
    {
        return MetricFactory.ofEnsembleBoxPlotCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume a {@link Pool} with single-valued 
     * pairs and produce {@link DurationDiagramStatisticOuter}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Double, Double>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter>
            ofSingleValuedTimeSeriesCollection( MetricConstants... metric )
    {
        return MetricFactory.ofSingleValuedTimeSeriesCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume single-valued pairs and produce
     * {@link DoubleScoreStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ofSingleValuedScoreCollection( ExecutorService executor,
                                           MetricConstants... metric )
    {
        final Builder<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> builder =
                Builder.of();

        // Add the metrics
        for ( MetricConstants next : metric )
        {
            if ( MetricFactory.isCollectable( next ) )
            {
                builder.addCollectableMetric( MetricFactory.ofSingleValuedScoreCollectable( next ) );
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
     * Returns a {@link MetricCollection} of metrics that consume single-valued pairs and produce
     * {@link DiagramStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static MetricCollection<Pool<Pair<Double, Double>>, DiagramStatisticOuter, DiagramStatisticOuter>
            ofSingleValuedDiagramCollection( ExecutorService executor,
                                             MetricConstants... metric )
    {
        final Builder<Pool<Pair<Double, Double>>, DiagramStatisticOuter, DiagramStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofSingleValuedDiagram( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume single-valued pairs and produce 
     * {@link BoxplotStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static MetricCollection<Pool<Pair<Double, Double>>, BoxplotStatisticOuter, BoxplotStatisticOuter>
            ofSingleValuedBoxPlotCollection( ExecutorService executor,
                                             MetricConstants... metric )
    {
        final Builder<Pool<Pair<Double, Double>>, BoxplotStatisticOuter, BoxplotStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofSingleValuedBoxPlot( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume discrete probability pairs and produce
     * {@link DoubleScoreStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ofDiscreteProbabilityScoreCollection( ExecutorService executor,
                                                  MetricConstants... metric )
    {
        final Builder<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofDiscreteProbabilityScore( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume dichotomous pairs and produce
     * {@link DoubleScoreStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static
            MetricCollection<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ofDichotomousScoreCollection( ExecutorService executor,
                                          MetricConstants... metric )
    {
        final Builder<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            // All dichotomous scores are collectable
            builder.addCollectableMetric( MetricFactory.ofDichotomousScore( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume discrete probability pairs and produce
     * {@link DiagramStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public static
            MetricCollection<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter, DiagramStatisticOuter>
            ofDiscreteProbabilityDiagramCollection( ExecutorService executor,
                                                    MetricConstants... metric )
    {
        final Builder<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter, DiagramStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofDiscreteProbabilityDiagram( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume ensemble pairs and produce 
     * {@link DoubleScoreStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
            ofEnsembleScoreCollection( ExecutorService executor,
                                       MetricConstants... metric )
    {
        final Builder<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofEnsembleScore( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume ensemble pairs and produce {@link DiagramStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<Pool<Pair<Double, Ensemble>>, DiagramStatisticOuter, DiagramStatisticOuter>
            ofEnsembleDiagramCollection( ExecutorService executor,
                                         MetricConstants... metric )
    {
        final Builder<Pool<Pair<Double, Ensemble>>, DiagramStatisticOuter, DiagramStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofEnsembleDiagram( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume ensemble pairs and produce
     * {@link BoxplotStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static MetricCollection<Pool<Pair<Double, Ensemble>>, BoxplotStatisticOuter, BoxplotStatisticOuter>
            ofEnsembleBoxPlotCollection( ExecutorService executor,
                                         MetricConstants... metric )
    {
        final Builder<Pool<Pair<Double, Ensemble>>, BoxplotStatisticOuter, BoxplotStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofEnsembleBoxPlot( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link Pool} with single-valued pairs 
     * and produce {@link DurationDiagramStatisticOuter}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public static
            MetricCollection<Pool<Pair<Double, Double>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter>
            ofSingleValuedTimeSeriesCollection( ExecutorService executor,
                                                MetricConstants... metric )
    {
        final Builder<Pool<Pair<Double, Double>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            builder.addMetric( MetricFactory.ofSingleValuedTimeSeries( next ) );
        }
        builder.setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link Metric} that consumes single-valued pairs and produces {@link DoubleScoreStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return the metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter>
            ofSingleValuedScore( MetricConstants metric )
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
            case MEDIAN_ERROR:
                return MedianError.of();
            case SAMPLE_SIZE:
                return SampleSize.of();
            case INDEX_OF_AGREEMENT:
                return IndexOfAgreement.of();
            case VOLUMETRIC_EFFICIENCY:
                return VolumetricEfficiency.of();
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return MeanSquareErrorSkillScore.of();
            case MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED:
                return MeanSquareErrorSkillScoreNormalized.of();
            case COEFFICIENT_OF_DETERMINATION:
                return CoefficientOfDetermination.of();
            case PEARSON_CORRELATION_COEFFICIENT:
                return CorrelationPearsons.of();
            case MEAN_SQUARE_ERROR:
                return MeanSquareError.of();
            case ROOT_MEAN_SQUARE_ERROR:
                return RootMeanSquareError.of();
            case ROOT_MEAN_SQUARE_ERROR_NORMALIZED:
                return RootMeanSquareErrorNormalized.of();
            case SUM_OF_SQUARE_ERROR:
                return SumOfSquareError.of();
            case MEAN:
                return Mean.of();
            case STANDARD_DEVIATION:
                return StandardDeviation.of();
            case MINIMUM:
                return Minimum.of();
            case MAXIMUM:
                return Maximum.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Collectable} that consumes single-valued pairs and produces {@link DoubleScoreStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return the metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
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
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return MeanSquareErrorSkillScore.of();
            case MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED:
                return MeanSquareErrorSkillScoreNormalized.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes single-valued pairs and produces {@link DiagramStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Double, Double>>, DiagramStatisticOuter>
            ofSingleValuedDiagram( MetricConstants metric )
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
     * Returns a {@link Metric} that consumes single-valued pairs and produces {@link BoxplotStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Double, Double>>, BoxplotStatisticOuter>
            ofSingleValuedBoxPlot( MetricConstants metric )
    {
        switch ( metric )
        {
            case BOX_PLOT_OF_ERRORS:
                return BoxPlotError.of();
            case BOX_PLOT_OF_PERCENTAGE_ERRORS:
                return BoxPlotPercentageError.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes discrete probability pairs and produces {@link DoubleScoreStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter>
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
     * Returns a {@link Metric} that consumes dichotomous pairs and produces {@link DoubleScoreStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Collectable<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
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
            case CONTINGENCY_TABLE:
                return ContingencyTable.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes discrete probability pairs and produces {@link DiagramStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter>
            ofDiscreteProbabilityDiagram( MetricConstants metric )
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
     * Returns a {@link Metric} that consumes ensemble pairs and produces {@link DoubleScoreStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter>
            ofEnsembleScore( MetricConstants metric )
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
     * Returns a {@link Metric} that consumes ensemble pairs and produces {@link BoxplotStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Double, Ensemble>>, BoxplotStatisticOuter>
            ofEnsembleBoxPlot( MetricConstants metric )
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
     * Returns a {@link Metric} that consumes ensemble pairs and produces {@link DiagramStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Double, Ensemble>>, DiagramStatisticOuter>
            ofEnsembleDiagram( MetricConstants metric )
    {
        switch ( metric )
        {
            case RANK_HISTOGRAM:
                Random random = MetricFactory.getRandomNumberGenerator();
                return RankHistogram.of( random );
            case ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM:
                return EnsembleQuantileQuantileDiagram.of();
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes a {@link Pool} with single-valued pairs and produces 
     * {@link DurationDiagramStatisticOuter}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Double, Double>>, DurationDiagramStatisticOuter>
            ofSingleValuedTimeSeries( MetricConstants metric )
    {
        // Use a random number generator with a fixed seed if required
        Random random = MetricFactory.getRandomNumberGenerator();

        switch ( metric )
        {
            case TIME_TO_PEAK_ERROR:
                return TimeToPeakError.of( random );
            case TIME_TO_PEAK_RELATIVE_ERROR:
                return TimeToPeakRelativeError.of( random );
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

        return singleValued || metric.isInGroup( SampleDataGroup.DICHOTOMOUS );
    }

    /**
     * @return a pseudo-random number generator that respects any {@link #TEST_SEED_PROPERTY}.
     */

    private static Random getRandomNumberGenerator()
    {
        // Use a fixed seed if required
        String seed = System.getProperty( MetricFactory.TEST_SEED_PROPERTY );
        if ( Objects.nonNull( seed ) )
        {
            long longSeed = Long.parseLong( seed );
            return new Random( longSeed );
        }

        return new Random();
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
