package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.inputs.MetricInput;
import wres.datamodel.inputs.pairs.DichotomousPairs;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.inputs.pairs.EnsemblePairs;
import wres.datamodel.inputs.pairs.MulticategoryPairs;
import wres.datamodel.inputs.pairs.PairedInput;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MultiValuedScoreOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.Metric.MetricBuilder;
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.SampleSize.SampleSizeBuilder;
import wres.engine.statistics.metric.categorical.ContingencyTable;
import wres.engine.statistics.metric.categorical.CriticalSuccessIndex;
import wres.engine.statistics.metric.categorical.EquitableThreatScore;
import wres.engine.statistics.metric.categorical.FrequencyBias;
import wres.engine.statistics.metric.categorical.FrequencyBias.FrequencyBiasBuilder;
import wres.engine.statistics.metric.config.MetricConfigurationException;
import wres.engine.statistics.metric.categorical.PeirceSkillScore;
import wres.engine.statistics.metric.categorical.ProbabilityOfDetection;
import wres.engine.statistics.metric.categorical.ProbabilityOfFalseDetection;
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

/**
 * <p>
 * A factory class for constructing metrics.
 * </p>
 * <p>
 * TODO: support construction with parameters by first defining a setParameters(EnumMap mapping) in the
 * {@link MetricBuilder} and then adding parametric methods here.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public class MetricFactory
{

    /**
     * Instance of an {@link DataFactory} for building metric outputs.
     */

    private DataFactory outputFactory = null;

    /**
     * Instance of the factory.
     */

    private static MetricFactory instance = null;

    /**
     * String used in several error messages.
     */

    private static final String error = "Unrecognized metric for identifier";

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
     * Returns an instance of a {@link MetricProcessor} for processing {@link SingleValuedPairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final MetricOutputGroup... mergeList )
                    throws MetricConfigurationException
    {
        return ofMetricProcessorByTimeSingleValuedPairs( config, null, null, mergeList );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link EnsemblePairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public MetricProcessorByTime<EnsemblePairs>
            ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                  final MetricOutputGroup... mergeList )
                    throws MetricConfigurationException
    {
        return ofMetricProcessorByTimeEnsemblePairs( config, null, null, mergeList );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link SingleValuedPairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final ExecutorService thresholdExecutor,
                                                      final ExecutorService metricExecutor,
                                                      final MetricOutputGroup... mergeList )
                    throws MetricConfigurationException
    {
        return new MetricProcessorByTimeSingleValuedPairs( outputFactory,
                                                           config,
                                                           thresholdExecutor,
                                                           metricExecutor,
                                                           mergeList );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link EnsemblePairs}. Optionally, retain 
     * and merge the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessorByTime}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public MetricProcessorByTime<EnsemblePairs> ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                                                      final ExecutorService thresholdExecutor,
                                                                                      final ExecutorService metricExecutor,
                                                                                      final MetricOutputGroup... mergeList )
            throws MetricConfigurationException
    {
        return new MetricProcessorByTimeEnsemblePairs( outputFactory,
                                                       config,
                                                       thresholdExecutor,
                                                       metricExecutor,
                                                       mergeList );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<SingleValuedPairs, ScalarOutput> ofSingleValuedScalarCollection( MetricConstants... metric )
            throws MetricParameterException
    {
        return ofSingleValuedScalarCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link MultiValuedScoreOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<SingleValuedPairs, MultiValuedScoreOutput>
            ofSingleValuedVectorCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return ofSingleValuedVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public MetricCollection<SingleValuedPairs, MultiVectorOutput>
            ofSingleValuedMultiVectorCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return ofSingleValuedMultiVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link MultiValuedScoreOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<DiscreteProbabilityPairs, MultiValuedScoreOutput>
            ofDiscreteProbabilityVectorCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return ofDiscreteProbabilityVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public MetricCollection<DichotomousPairs, ScalarOutput> ofDichotomousScalarCollection( MetricConstants... metric )
            throws MetricParameterException
    {
        return ofDichotomousScalarCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput>
            ofDiscreteProbabilityMultiVectorCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return ofDiscreteProbabilityMultiVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link MulticategoryPairs} and produce
     * {@link MatrixOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public MetricCollection<MulticategoryPairs, MatrixOutput>
            ofMulticategoryMatrixCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return ofMulticategoryMatrixCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<EnsemblePairs, ScalarOutput> ofEnsembleScalarCollection( MetricConstants... metric )
            throws MetricParameterException
    {
        return ofEnsembleScalarCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link MultiValuedScoreOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<EnsemblePairs, MultiValuedScoreOutput>
            ofEnsembleVectorCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return ofEnsembleVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<EnsemblePairs, MultiVectorOutput>
            ofEnsembleMultiVectorCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return ofEnsembleMultiVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link BoxPlotOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<EnsemblePairs, BoxPlotOutput>
            ofEnsembleBoxPlotCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return ofEnsembleBoxPlotCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<SingleValuedPairs, ScalarOutput> ofSingleValuedScalarCollection( ExecutorService executor,
                                                                                             MetricConstants... metric )
            throws MetricParameterException
    {
        final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput> builder = MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofSingleValuedScalar( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link MultiValuedScoreOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<SingleValuedPairs, MultiValuedScoreOutput>
            ofSingleValuedVectorCollection( ExecutorService executor,
                                            MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<SingleValuedPairs, MultiValuedScoreOutput> builder = MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofSingleValuedVector( next ) );
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
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<SingleValuedPairs, MultiVectorOutput>
            ofSingleValuedMultiVectorCollection( ExecutorService executor,
                                                 MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<SingleValuedPairs, MultiVectorOutput> builder = MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofSingleValuedMultiVector( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link MultiValuedScoreOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<DiscreteProbabilityPairs, MultiValuedScoreOutput>
            ofDiscreteProbabilityVectorCollection( ExecutorService executor,
                                                   MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, MultiValuedScoreOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofDiscreteProbabilityVector( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<DichotomousPairs, ScalarOutput> ofDichotomousScalarCollection( ExecutorService executor,
                                                                                           MetricConstants... metric )
            throws MetricParameterException
    {
        final MetricCollectionBuilder<DichotomousPairs, ScalarOutput> builder = MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofDichotomousScalar( next ) );
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
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput>
            ofDiscreteProbabilityMultiVectorCollection( ExecutorService executor,
                                                        MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, MultiVectorOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofDiscreteProbabilityMultiVector( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link MulticategoryPairs} and produce
     * {@link MatrixOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<MulticategoryPairs, MatrixOutput> ofMulticategoryMatrixCollection( ExecutorService executor,
                                                                                               MetricConstants... metric )
            throws MetricParameterException
    {
        final MetricCollectionBuilder<MulticategoryPairs, MatrixOutput> builder = MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofMulticategoryMatrix( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public MetricCollection<EnsemblePairs, ScalarOutput> ofEnsembleScalarCollection( ExecutorService executor,
                                                                                     MetricConstants... metric )
            throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, ScalarOutput> builder = MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofEnsembleScalar( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link MultiValuedScoreOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public MetricCollection<EnsemblePairs, MultiValuedScoreOutput> ofEnsembleVectorCollection( ExecutorService executor,
                                                                                               MetricConstants... metric )
            throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, MultiValuedScoreOutput> builder = MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofEnsembleVector( next ) );
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
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public MetricCollection<EnsemblePairs, MultiVectorOutput> ofEnsembleMultiVectorCollection( ExecutorService executor,
                                                                                               MetricConstants... metric )
            throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, MultiVectorOutput> builder = MetricCollectionBuilder.of();
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
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public MetricCollection<EnsemblePairs, BoxPlotOutput> ofEnsembleBoxPlotCollection( ExecutorService executor,
                                                                                       MetricConstants... metric )
            throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, BoxPlotOutput> builder = MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            builder.add( ofEnsembleBoxPlot( next ) );
        }
        builder.setOutputFactory( outputFactory ).setExecutorService( executor );
        return builder.build();
    }

    /**
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link ScalarOutput}.
     * 
     * @param metric the metric identifier
     * @return the metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized 
     */

    public Metric<SingleValuedPairs, ScalarOutput> ofSingleValuedScalar( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case BIAS_FRACTION:
                return ofBiasFraction();
            case MEAN_ABSOLUTE_ERROR:
                return ofMeanAbsoluteError();
            case MEAN_ERROR:
                return ofMeanError();
            case ROOT_MEAN_SQUARE_ERROR:
                return ofRootMeanSquareError();
            case PEARSON_CORRELATION_COEFFICIENT:
                return ofCorrelationPearsons();
            case COEFFICIENT_OF_DETERMINATION:
                return ofCoefficientOfDetermination();
            case SAMPLE_SIZE:
                return ofSampleSize();
            case INDEX_OF_AGREEMENT:
                return ofIndexOfAgreement();
            case VOLUMETRIC_EFFICIENCY:
                return ofVolumetricEfficiency();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link MultiValuedScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public Metric<SingleValuedPairs, MultiValuedScoreOutput> ofSingleValuedVector( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case MEAN_SQUARE_ERROR:
                return ofMeanSquareError();
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return ofMeanSquareErrorSkillScore();
            case KLING_GUPTA_EFFICIENCY:
                return ofKlingGuptaEfficiency();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
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
            throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DiscreteProbabilityPairs} and produces {@link MultiValuedScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public Metric<DiscreteProbabilityPairs, MultiValuedScoreOutput>
            ofDiscreteProbabilityVector( MetricConstants metric )
                    throws MetricParameterException
    {
        switch ( metric )
        {
            case BRIER_SCORE:
                return ofBrierScore();
            case BRIER_SKILL_SCORE:
                return ofBrierSkillScore();
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE:
                return ofRelativeOperatingCharacteristicScore();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DichotomousPairs} and produces {@link ScalarOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public Metric<DichotomousPairs, ScalarOutput> ofDichotomousScalar( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case CRITICAL_SUCCESS_INDEX:
                return ofCriticalSuccessIndex();
            case EQUITABLE_THREAT_SCORE:
                return ofEquitableThreatScore();
            case PEIRCE_SKILL_SCORE:
                return ofPeirceSkillScore();
            case PROBABILITY_OF_DETECTION:
                return ofProbabilityOfDetection();
            case PROBABILITY_OF_FALSE_DETECTION:
                return ofProbabilityOfFalseDetection();
            case FREQUENCY_BIAS:
                return ofFrequencyBias();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link MulticategoryPairs} and produces {@link ScalarOutput}. Use
     * {@link #ofDichotomousScalar(MetricConstants)} when the inputs are dichotomous.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public Metric<MulticategoryPairs, ScalarOutput> ofMulticategoryScalar( MetricConstants metric )
            throws MetricParameterException
    {
        if ( MetricConstants.PEIRCE_SKILL_SCORE.equals( metric ) )
        {
            return ofPeirceSkillScoreMulti();
        }
        else
        {
            throw new IllegalArgumentException( error + " '" + metric + "'." );
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
            case RELIABILITY_DIAGRAM:
                return ofReliabilityDiagram();
            case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM:
                return ofRelativeOperatingCharacteristic();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link MulticategoryPairs} and produces {@link MatrixOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public Metric<MulticategoryPairs, MatrixOutput> ofMulticategoryMatrix( MetricConstants metric )
            throws MetricParameterException
    {
        if ( MetricConstants.CONTINGENCY_TABLE.equals( metric ) )
        {
            return ofContingencyTable();
        }
        else
        {
            throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link ScalarOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public Metric<EnsemblePairs, ScalarOutput> ofEnsembleScalar( MetricConstants metric )
            throws MetricParameterException
    {
        if ( MetricConstants.SAMPLE_SIZE.equals( metric ) )
        {
            return ofSampleSize();
        }
        else
        {
            throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link MultiValuedScoreOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public Metric<EnsemblePairs, MultiValuedScoreOutput> ofEnsembleVector( MetricConstants metric )
            throws MetricParameterException
    {
        switch ( metric )
        {
            case CONTINUOUS_RANKED_PROBABILITY_SCORE:
                return ofContinuousRankedProbabilityScore();
            case CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE:
                return ofContinuousRankedProbabilitySkillScore();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
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
                return ofBoxPlotErrorByObserved();
            case BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE:
                return ofBoxPlotErrorByForecast();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
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
            throw new IllegalArgumentException( error + " '" + metric + "'." );
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
        return (BiasFraction) new BiasFraction.BiasFractionBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BrierScore ofBrierScore() throws MetricParameterException
    {
        return (BrierScore) new BrierScore.BrierScoreBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BrierSkillScore ofBrierSkillScore() throws MetricParameterException
    {
        return (BrierSkillScore) new BrierSkillScore.BrierSkillScoreBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link CoefficientOfDetermination} function.
     * 
     * @return a default {@link CoefficientOfDetermination} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public CorrelationPearsons ofCoefficientOfDetermination() throws MetricParameterException
    {
        return (CoefficientOfDetermination) new CoefficientOfDetermination.CoefficientOfDeterminationBuilder().setOutputFactory( outputFactory )
                                                                                                              .build();
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContingencyTable<MulticategoryPairs> ofContingencyTable() throws MetricParameterException
    {
        return (ContingencyTable<MulticategoryPairs>) new ContingencyTable.ContingencyTableBuilder<>().setOutputFactory( outputFactory )
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
        return (CorrelationPearsons) new CorrelationPearsons.CorrelationPearsonsBuilder().setOutputFactory( outputFactory )
                                                                                         .build();
    }

    /**
     * Return a default {@link CriticalSuccessIndex} function.
     * 
     * @return a default {@link CriticalSuccessIndex} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public CriticalSuccessIndex ofCriticalSuccessIndex() throws MetricParameterException
    {
        return (CriticalSuccessIndex) new CriticalSuccessIndex.CriticalSuccessIndexBuilder().setOutputFactory( outputFactory )
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
        return (MeanAbsoluteError) new MeanAbsoluteError.MeanAbsoluteErrorBuilder().setOutputFactory( outputFactory )
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
        return (MeanError) new MeanError.MeanErrorBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public MeanSquareError<SingleValuedPairs> ofMeanSquareError() throws MetricParameterException
    {
        return (MeanSquareError<SingleValuedPairs>) new MeanSquareError.MeanSquareErrorBuilder<>().setOutputFactory( outputFactory )
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
        return (MeanSquareErrorSkillScore<SingleValuedPairs>) new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>().setOutputFactory( outputFactory )
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
        return (RootMeanSquareError) new RootMeanSquareError.RootMeanSquareErrorBuilder().setOutputFactory( outputFactory )
                                                                                         .build();
    }

    /**
     * Return a default {@link ReliabilityDiagram} function.
     * 
     * @return a default {@link ReliabilityDiagram} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ReliabilityDiagram ofReliabilityDiagram() throws MetricParameterException
    {
        return (ReliabilityDiagram) new ReliabilityDiagramBuilder().setOutputFactory( outputFactory ).build();
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

    public ContinuousRankedProbabilityScore ofContinuousRankedProbabilitySkillScore() throws MetricParameterException
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
        return (FrequencyBias) new FrequencyBiasBuilder().setOutputFactory( outputFactory ).build();
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
