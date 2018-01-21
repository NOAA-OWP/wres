package wres.engine.statistics.metric;

import java.util.EnumMap;
import java.util.Map;
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
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.SampleSize.SampleSizeBuilder;
import wres.engine.statistics.metric.categorical.ContingencyTable;
import wres.engine.statistics.metric.categorical.CriticalSuccessIndex;
import wres.engine.statistics.metric.categorical.EquitableThreatScore;
import wres.engine.statistics.metric.categorical.FrequencyBias;
import wres.engine.statistics.metric.categorical.PeirceSkillScore;
import wres.engine.statistics.metric.categorical.ProbabilityOfDetection;
import wres.engine.statistics.metric.categorical.ProbabilityOfFalseDetection;
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
     * String used in several error messages.
     */

    private static final String error = "Unrecognized metric for identifier";

    /**
     * Instance of an {@link DataFactory} for building metric outputs.
     */

    private DataFactory outputFactory = null;

    /**
     * Cached {@link Metric} that consume {@link SingleValuedPairs} and produce {@link ScalarOutput}. 
     */

    private Map<MetricConstants, Metric<SingleValuedPairs, ScalarOutput>> singleValuedScalar;

    /**
     * Cached {@link Metric} that consume {@link SingleValuedPairs} and produce {@link MultiValuedScoreOutput}. 
     */

    private Map<MetricConstants, Metric<SingleValuedPairs, MultiValuedScoreOutput>> singleValuedVector;

    /**
     * Cached {@link Metric} that consume {@link EnsemblePairs} and produce {@link MultiValuedScoreOutput}. 
     */

    private Map<MetricConstants, Metric<EnsemblePairs, MultiValuedScoreOutput>> ensembleVector;

    /**
     * Cached {@link Collectable} that consume {@link SingleValuedPairs} and produce {@link ScalarOutput}. 
     */

    private Map<MetricConstants, Collectable<SingleValuedPairs, ScalarOutput, ScalarOutput>> singleValuedScalarCol;

    /**
     * Cached {@link Collectable} that consume {@link DichotomousPairs} and produce {@link ScalarOutput}. 
     */

    private Map<MetricConstants, Collectable<DichotomousPairs, MatrixOutput, ScalarOutput>> dichotomousScalarCol;

    /**
     * Cached {@link Metric} that consume {@link DiscreteProbabilityPairs} and produce {@link MultiValuedScoreOutput}. 
     */

    private Map<MetricConstants, Metric<DiscreteProbabilityPairs, MultiValuedScoreOutput>> discreteProbabilityVector;

    /**
     * Cached {@link Metric} that consume {@link DiscreteProbabilityPairs} and produce {@link MultiVectorOutput}. 
     */

    private Map<MetricConstants, Metric<DiscreteProbabilityPairs, MultiVectorOutput>> discreteProbabilityMultiVector;

    /**
     * Cached {@link Metric} that consume {@link EnsemblePairs} and produce {@link BoxPlotOutput}. 
     */

    private Map<MetricConstants, Metric<EnsemblePairs, BoxPlotOutput>> ensembleBoxPlot;

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
                                                     ForkJoinPool.commonPool(),
                                                     ForkJoinPool.commonPool(),
                                                     mergeList );
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
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<SingleValuedPairs>
            ofMetricProcessorByTimeSingleValuedPairs( final ProjectConfig config,
                                                      final ExecutorService thresholdExecutor,
                                                      final ExecutorService metricExecutor,
                                                      final MetricOutputGroup... mergeList )
                    throws MetricProcessorException
    {
        try
        {
            return new MetricProcessorByTimeSingleValuedPairs( outputFactory,
                                                               config,
                                                               thresholdExecutor,
                                                               metricExecutor,
                                                               mergeList );
        }
        catch ( MetricConfigurationException e )
        {
            throw new MetricProcessorException( "While building the metric processor, a configuration exception "
                                                + "occurred: ", e );
        }
        catch ( MetricParameterException e )
        {
            throw new MetricProcessorException( "While building the metric processor, a parameter exception occurred: ",
                                                e );
        }
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
     * @throws MetricProcessorException if the metric processor could not be built
     */

    public MetricProcessorByTime<EnsemblePairs> ofMetricProcessorByTimeEnsemblePairs( final ProjectConfig config,
                                                                                      final ExecutorService thresholdExecutor,
                                                                                      final ExecutorService metricExecutor,
                                                                                      final MetricOutputGroup... mergeList )
            throws MetricProcessorException
    {
        try
        {
            return new MetricProcessorByTimeEnsemblePairs( outputFactory,
                                                           config,
                                                           thresholdExecutor,
                                                           metricExecutor,
                                                           mergeList );
        }
        catch ( MetricConfigurationException e )
        {
            throw new MetricProcessorException( "While building the metric processor, a configuration exception "
                                                + "occurred: ", e );
        }
        catch ( MetricParameterException e )
        {
            throw new MetricProcessorException( "While building the metric processor, a parameter exception occurred: ",
                                                e );
        }
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<SingleValuedPairs, ScalarOutput, ScalarOutput>
            ofSingleValuedScalarCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return ofSingleValuedScalarCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link MultiValuedScoreOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<SingleValuedPairs, MultiValuedScoreOutput, MultiValuedScoreOutput>
            ofSingleValuedVectorCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return ofSingleValuedVectorCollection( ForkJoinPool.commonPool(), metric );
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
     * {@link MultiValuedScoreOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<DiscreteProbabilityPairs, MultiValuedScoreOutput, MultiValuedScoreOutput>
            ofDiscreteProbabilityVectorCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return ofDiscreteProbabilityVectorCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<DichotomousPairs, MatrixOutput, ScalarOutput>
            ofDichotomousScalarCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return ofDichotomousScalarCollection( ForkJoinPool.commonPool(), metric );
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
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link MulticategoryPairs} and produce
     * {@link MatrixOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<MulticategoryPairs, MatrixOutput, MatrixOutput>
            ofMulticategoryMatrixCollection( MetricConstants... metric ) throws MetricParameterException
    {
        return ofMulticategoryMatrixCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link ScalarOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public MetricCollection<EnsemblePairs, ScalarOutput, ScalarOutput>
            ofEnsembleScalarCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return ofEnsembleScalarCollection( ForkJoinPool.commonPool(), metric );
    }

    /**
     * <p>Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link MultiValuedScoreOutput}.</p>
     * 
     * <p>Uses the {@link ForkJoinPool#commonPool()} for execution.</p>
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public MetricCollection<EnsemblePairs, MultiValuedScoreOutput, MultiValuedScoreOutput>
            ofEnsembleVectorCollection( MetricConstants... metric )
                    throws MetricParameterException
    {
        return ofEnsembleVectorCollection( ForkJoinPool.commonPool(), metric );
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
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<SingleValuedPairs, ScalarOutput, ScalarOutput>
            ofSingleValuedScalarCollection( ExecutorService executor,
                                            MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<SingleValuedPairs, ScalarOutput, ScalarOutput> builder =
                MetricCollectionBuilder.of();
        // Build the store if required
        buildSingleValuedScalarStore();
        for ( MetricConstants next : metric )
        {
            if ( singleValuedScalarCol.containsKey( next ) )
            {
                builder.add( singleValuedScalarCol.get( next ) );
            }
            else if ( singleValuedScalar.containsKey( next ) )
            {
                builder.add( singleValuedScalar.get( next ) );
            }
            else
            {
                throw new IllegalArgumentException( error + " '" + metric + "'." );
            }
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
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<SingleValuedPairs, MultiValuedScoreOutput, MultiValuedScoreOutput>
            ofSingleValuedVectorCollection( ExecutorService executor,
                                            MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<SingleValuedPairs, MultiValuedScoreOutput, MultiValuedScoreOutput> builder =
                MetricCollectionBuilder.of();
        // Build store if required
        buildSingleValuedVectorStore();
        for ( MetricConstants next : metric )
        {
            if ( !singleValuedVector.containsKey( next ) )
            {
                throw new IllegalArgumentException( error + " '" + metric + "'." );
            }
            builder.add( singleValuedVector.get( next ) );
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
     * {@link MultiValuedScoreOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     * @throws MetricParameterException if one or more parameter values is incorrect
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<DiscreteProbabilityPairs, MultiValuedScoreOutput, MultiValuedScoreOutput>
            ofDiscreteProbabilityVectorCollection( ExecutorService executor,
                                                   MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, MultiValuedScoreOutput, MultiValuedScoreOutput> builder =
                MetricCollectionBuilder.of();
        // Build the store if required
        buildDiscreteProbabilityVectorStore();
        for ( MetricConstants next : metric )
        {
            if ( !discreteProbabilityVector.containsKey( next ) )
            {
                throw new IllegalArgumentException( error + " '" + metric + "'." );
            }
            builder.add( discreteProbabilityVector.get( next ) );
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
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public MetricCollection<DichotomousPairs, MatrixOutput, ScalarOutput>
            ofDichotomousScalarCollection( ExecutorService executor,
                                           MetricConstants... metric )
                    throws MetricParameterException
    {
        // Build store if required
        buildDichotomousScalarStore();
        final MetricCollectionBuilder<DichotomousPairs, MatrixOutput, ScalarOutput> builder =
                MetricCollectionBuilder.of();
        for ( MetricConstants next : metric )
        {
            if ( !dichotomousScalarCol.containsKey( next ) )
            {
                throw new IllegalArgumentException( error + " '" + metric + "'." );
            }
            builder.add( dichotomousScalarCol.get( next ) );
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
                throw new IllegalArgumentException( error + " '" + metric + "'." );
            }
            builder.add( discreteProbabilityMultiVector.get( next ) );
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
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public MetricCollection<MulticategoryPairs, MatrixOutput, MatrixOutput>
            ofMulticategoryMatrixCollection( ExecutorService executor,
                                             MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<MulticategoryPairs, MatrixOutput, MatrixOutput> builder =
                MetricCollectionBuilder.of();
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
     * @throws IllegalArgumentException if a metric identifier is not recognized
     */

    public MetricCollection<EnsemblePairs, ScalarOutput, ScalarOutput>
            ofEnsembleScalarCollection( ExecutorService executor,
                                        MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, ScalarOutput, ScalarOutput> builder =
                MetricCollectionBuilder.of();
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
     * @throws IllegalArgumentException if a metric identifier is not recognized 
     */

    public MetricCollection<EnsemblePairs, MultiValuedScoreOutput, MultiValuedScoreOutput>
            ofEnsembleVectorCollection( ExecutorService executor,
                                        MetricConstants... metric )
                    throws MetricParameterException
    {
        final MetricCollectionBuilder<EnsemblePairs, MultiValuedScoreOutput, MultiValuedScoreOutput> builder =
                MetricCollectionBuilder.of();
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
                throw new IllegalArgumentException( error + " '" + metric + "'." );
            }
            builder.add( ensembleBoxPlot.get( next ) );
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
        // Build store if required
        buildSingleValuedScalarStore();
        if ( singleValuedScalarCol.containsKey( metric ) )
        {
            return singleValuedScalarCol.get( metric );
        }
        else if ( singleValuedScalar.containsKey( metric ) )
        {
            return singleValuedScalar.get( metric );
        }
        else
        {
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
        // Build store if required
        buildSingleValuedVectorStore();
        if ( singleValuedVector.containsKey( metric ) )
        {
            return singleValuedVector.get( metric );
        }
        else
        {
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
        // Build store if required
        buildDiscreteProbabilityVectorStore();
        if ( discreteProbabilityVector.containsKey( metric ) )
        {
            return discreteProbabilityVector.get( metric );
        }
        else
        {
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
        // Build store if required
        buildDichotomousScalarStore();
        if ( dichotomousScalarCol.containsKey( metric ) )
        {
            return dichotomousScalarCol.get( metric );
        }
        else
        {
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
        // Build store if required
        buildDiscreteProbabilityMultiVectorStore();
        if ( discreteProbabilityMultiVector.containsKey( metric ) )
        {
            return discreteProbabilityMultiVector.get( metric );
        }
        else
        {
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
        // Build store if required
        buildEnsembleVectorStore();
        if ( ensembleVector.containsKey( metric ) )
        {
            return ensembleVector.get( metric );
        }
        else
        {
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
        // Build store if required
        buildEnsembleBoxPlotStore();
        if ( ensembleBoxPlot.containsKey( metric ) )
        {
            return ensembleBoxPlot.get( metric );
        }
        else
        {
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
        buildSingleValuedScalarStore();
        return (BiasFraction) singleValuedScalar.get( MetricConstants.BIAS_FRACTION );
    }

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BrierScore ofBrierScore() throws MetricParameterException
    {
        buildDiscreteProbabilityVectorStore();
        return (BrierScore) discreteProbabilityVector.get( MetricConstants.BRIER_SCORE );
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public BrierSkillScore ofBrierSkillScore() throws MetricParameterException
    {
        buildDiscreteProbabilityVectorStore();
        return (BrierSkillScore) discreteProbabilityVector.get( MetricConstants.BRIER_SKILL_SCORE );
    }

    /**
     * Return a default {@link CoefficientOfDetermination} function.
     * 
     * @return a default {@link CoefficientOfDetermination} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public CorrelationPearsons ofCoefficientOfDetermination() throws MetricParameterException
    {
        buildSingleValuedScalarStore();
        return (CoefficientOfDetermination) singleValuedScalarCol.get( MetricConstants.COEFFICIENT_OF_DETERMINATION );
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContingencyTable<MulticategoryPairs> ofContingencyTable() throws MetricParameterException
    {
        return new ContingencyTable.ContingencyTableBuilder<>().setOutputFactory( outputFactory )
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
        buildSingleValuedScalarStore();
        return (CorrelationPearsons) singleValuedScalarCol.get( MetricConstants.PEARSON_CORRELATION_COEFFICIENT );
    }

    /**
     * Return a default {@link CriticalSuccessIndex} function.
     * 
     * @return a default {@link CriticalSuccessIndex} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public CriticalSuccessIndex ofCriticalSuccessIndex() throws MetricParameterException
    {
        buildDichotomousScalarStore();
        return (CriticalSuccessIndex) dichotomousScalarCol.get( MetricConstants.CRITICAL_SUCCESS_INDEX );
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
        return (EquitableThreatScore) dichotomousScalarCol.get( MetricConstants.EQUITABLE_THREAT_SCORE );
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public MeanAbsoluteError ofMeanAbsoluteError() throws MetricParameterException
    {
        buildSingleValuedScalarStore();
        return (MeanAbsoluteError) singleValuedScalar.get( MetricConstants.MEAN_ABSOLUTE_ERROR );
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public MeanError ofMeanError() throws MetricParameterException
    {
        buildSingleValuedScalarStore();
        return (MeanError) singleValuedScalar.get( MetricConstants.MEAN_ERROR );
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function
     * @throws MetricParameterException if one or more parameter values is incorrect 
     */

    public MeanSquareError<SingleValuedPairs> ofMeanSquareError() throws MetricParameterException
    {
        buildSingleValuedVectorStore();
        return (MeanSquareError<SingleValuedPairs>) singleValuedVector.get( MetricConstants.MEAN_SQUARE_ERROR );
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public MeanSquareErrorSkillScore<SingleValuedPairs> ofMeanSquareErrorSkillScore() throws MetricParameterException
    {
        buildSingleValuedVectorStore();
        return (MeanSquareErrorSkillScore<SingleValuedPairs>) singleValuedVector.get( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE );
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
        return (PeirceSkillScore<DichotomousPairs>) dichotomousScalarCol.get( MetricConstants.PEIRCE_SKILL_SCORE );
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
        return (ProbabilityOfDetection) dichotomousScalarCol.get( MetricConstants.PROBABILITY_OF_DETECTION );
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
        return (ProbabilityOfFalseDetection) dichotomousScalarCol.get( MetricConstants.PROBABILITY_OF_FALSE_DETECTION );
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
        buildSingleValuedScalarStore();
        return (RootMeanSquareError) singleValuedScalar.get( MetricConstants.ROOT_MEAN_SQUARE_ERROR );
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
        buildDiscreteProbabilityVectorStore();
        return (RelativeOperatingCharacteristicScore) discreteProbabilityVector.get( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE );
    }

    /**
     * Return a default {@link ContinuousRankedProbabilityScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilityScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContinuousRankedProbabilityScore ofContinuousRankedProbabilityScore() throws MetricParameterException
    {
        buildEnsembleVectorStore();
        return (ContinuousRankedProbabilityScore) ensembleVector.get( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE );
    }

    /**
     * Return a default {@link ContinuousRankedProbabilitySkillScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilitySkillScore} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public ContinuousRankedProbabilityScore ofContinuousRankedProbabilitySkillScore() throws MetricParameterException
    {
        buildEnsembleVectorStore();
        return (ContinuousRankedProbabilitySkillScore) ensembleVector.get( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
    }

    /**
     * Return a default {@link IndexOfAgreement} function.
     * 
     * @return a default {@link IndexOfAgreement} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public IndexOfAgreement ofIndexOfAgreement() throws MetricParameterException
    {
        buildSingleValuedScalarStore();
        return (IndexOfAgreement) singleValuedScalar.get( MetricConstants.INDEX_OF_AGREEMENT );
    }

    /**
     * Return a default {@link KlingGuptaEfficiency} function.
     * 
     * @return a default {@link KlingGuptaEfficiency} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public KlingGuptaEfficiency ofKlingGuptaEfficiency() throws MetricParameterException
    {
        buildSingleValuedVectorStore();
        return (KlingGuptaEfficiency) singleValuedVector.get( MetricConstants.KLING_GUPTA_EFFICIENCY );
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
        return (FrequencyBias) dichotomousScalarCol.get( MetricConstants.FREQUENCY_BIAS );
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
        buildSingleValuedScalarStore();
        return (VolumetricEfficiency) singleValuedScalar.get( MetricConstants.VOLUMETRIC_EFFICIENCY );
    }

    /**
     * Return a default {@link TimeToPeakError} function.
     * 
     * @return a default {@link TimeToPeakError} function
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    public TimeToPeakError ofTimeToPeakError() throws MetricParameterException
    {
        return new TimeToPeakErrorBuilder().setOutputFactory( outputFactory ).build();
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
     * Builds the store of metrics that consume {@link SingleValuedPairs} and produce {@link ScalarOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildSingleValuedScalarStore() throws MetricParameterException
    {
        if ( Objects.isNull( singleValuedScalar ) )
        {
            // Ordinary metrics
            singleValuedScalar = new EnumMap<>( MetricConstants.class );
            singleValuedScalar.put( MetricConstants.BIAS_FRACTION,
                                    new BiasFraction.BiasFractionBuilder().setOutputFactory( outputFactory ).build() );
            singleValuedScalar.put( MetricConstants.MEAN_ABSOLUTE_ERROR,
                                    new MeanAbsoluteError.MeanAbsoluteErrorBuilder().setOutputFactory( outputFactory )
                                                                                    .build() );
            singleValuedScalar.put( MetricConstants.MEAN_ERROR,
                                    new MeanError.MeanErrorBuilder().setOutputFactory( outputFactory )
                                                                    .build() );
            singleValuedScalar.put( MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                                    new RootMeanSquareError.RootMeanSquareErrorBuilder().setOutputFactory( outputFactory )
                                                                                        .build() );
            singleValuedScalar.put( MetricConstants.SAMPLE_SIZE,
                                    new SampleSizeBuilder<SingleValuedPairs>().setOutputFactory( outputFactory )
                                                                              .build() );
            singleValuedScalar.put( MetricConstants.INDEX_OF_AGREEMENT,
                                    new IndexOfAgreementBuilder().setOutputFactory( outputFactory ).build() );
            singleValuedScalar.put( MetricConstants.VOLUMETRIC_EFFICIENCY,
                                    new VolumetricEfficiencyBuilder().setOutputFactory( outputFactory ).build() );
            // Collectable metrics
            singleValuedScalarCol = new EnumMap<>( MetricConstants.class );
            singleValuedScalarCol.put( MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                       (CoefficientOfDetermination) new CoefficientOfDetermination.CoefficientOfDeterminationBuilder().setOutputFactory( outputFactory )
                                                                                                                                      .build() );
            singleValuedScalarCol.put( MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                       (CorrelationPearsons) new CorrelationPearsons.CorrelationPearsonsBuilder().setOutputFactory( outputFactory )
                                                                                                                 .build() );
        }
    }

    /**
     * Builds the store of metrics that consume {@link SingleValuedPairs} and produce {@link MultiValuedScoreOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildSingleValuedVectorStore() throws MetricParameterException
    {
        if ( Objects.isNull( singleValuedVector ) )
        {
            singleValuedVector = new EnumMap<>( MetricConstants.class );
            singleValuedVector.put( MetricConstants.MEAN_SQUARE_ERROR,
                                    new MeanSquareError.MeanSquareErrorBuilder<>().setOutputFactory( outputFactory )
                                                                                  .build() );
            singleValuedVector.put( MetricConstants.MEAN_SQUARE_ERROR_SKILL_SCORE,
                                    new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>().setOutputFactory( outputFactory )
                                                                                                      .build() );
            singleValuedVector.put( MetricConstants.KLING_GUPTA_EFFICIENCY,
                                    new KlingGuptaEfficiencyBuilder().setOutputFactory( outputFactory ).build() );
        }
    }

    /**
     * Builds the store of metrics that consume {@link DichotomousPairs} and produce {@link ScalarOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildDichotomousScalarStore() throws MetricParameterException
    {
        if ( Objects.isNull( dichotomousScalarCol ) )
        {
            dichotomousScalarCol = new EnumMap<>( MetricConstants.class );
            dichotomousScalarCol.put( MetricConstants.CRITICAL_SUCCESS_INDEX,
                                      (CriticalSuccessIndex) new CriticalSuccessIndex.CriticalSuccessIndexBuilder().setOutputFactory( outputFactory )
                                                                                                                   .build() );
            dichotomousScalarCol.put( MetricConstants.EQUITABLE_THREAT_SCORE,
                                      (EquitableThreatScore) new EquitableThreatScore.EquitableThreatScoreBuilder().setOutputFactory( outputFactory )
                                                                                                                   .build() );
            dichotomousScalarCol.put( MetricConstants.PEIRCE_SKILL_SCORE,
                                      (PeirceSkillScore<DichotomousPairs>) new PeirceSkillScore.PeirceSkillScoreBuilder<DichotomousPairs>().setOutputFactory( outputFactory )
                                                                                                                                           .build() );
            dichotomousScalarCol.put( MetricConstants.PROBABILITY_OF_DETECTION,
                                      (ProbabilityOfDetection) new ProbabilityOfDetection.ProbabilityOfDetectionBuilder().setOutputFactory( outputFactory )
                                                                                                                         .build() );
            dichotomousScalarCol.put( MetricConstants.PROBABILITY_OF_FALSE_DETECTION,
                                      (ProbabilityOfFalseDetection) new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder().setOutputFactory( outputFactory )
                                                                                                                                        .build() );
            dichotomousScalarCol.put( MetricConstants.FREQUENCY_BIAS,
                                      (FrequencyBias) new FrequencyBias.FrequencyBiasBuilder().setOutputFactory( outputFactory )
                                                                                              .build() );
        }
    }

    /**
     * Builds the store of metrics that consume {@link EnsemblePairs} and produce {@link MultiValuedScoreOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildEnsembleVectorStore() throws MetricParameterException
    {
        if ( Objects.isNull( ensembleVector ) )
        {
            ensembleVector = new EnumMap<>( MetricConstants.class );
            ensembleVector.put( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SCORE,
                                new CRPSBuilder().setOutputFactory( outputFactory ).build() );
            ensembleVector.put( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                new CRPSSBuilder().setOutputFactory( outputFactory ).build() );
        }
    }

    /**
     * Builds the store of metrics that consume {@link DiscreteProbabilityPairs} and produce 
     * {@link MultiValuedScoreOutput}. 
     * @throws MetricParameterException if one or more parameter values is incorrect
     */

    private void buildDiscreteProbabilityVectorStore() throws MetricParameterException
    {
        if ( Objects.isNull( discreteProbabilityVector ) )
        {
            discreteProbabilityVector = new EnumMap<>( MetricConstants.class );
            discreteProbabilityVector.put( MetricConstants.BRIER_SCORE,
                                           new BrierScore.BrierScoreBuilder().setOutputFactory( outputFactory )
                                                                             .build() );
            discreteProbabilityVector.put( MetricConstants.BRIER_SKILL_SCORE,
                                           new BrierSkillScore.BrierSkillScoreBuilder().setOutputFactory( outputFactory )
                                                                                       .build() );
            discreteProbabilityVector.put( MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_SCORE,
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
}
