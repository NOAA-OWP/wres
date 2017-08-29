package wres.engine.statistics.metric;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DichotomousPairs;
import wres.datamodel.DiscreteProbabilityPairs;
import wres.datamodel.EnsemblePairs;
import wres.datamodel.MatrixOutput;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricInput;
import wres.datamodel.MultiVectorOutput;
import wres.datamodel.MulticategoryPairs;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.datamodel.VectorOutput;
import wres.engine.statistics.metric.ContinuousRankedProbabilityScore.CRPSBuilder;
import wres.engine.statistics.metric.ContinuousRankedProbabilitySkillScore.CRPSSBuilder;
import wres.engine.statistics.metric.FrequencyBias.FrequencyBiasBuilder;
import wres.engine.statistics.metric.IndexOfAgreement.IndexOfAgreementBuilder;
import wres.engine.statistics.metric.KlingGuptaEfficiency.KlingGuptaEfficiencyBuilder;
import wres.engine.statistics.metric.Metric.MetricBuilder;
import wres.engine.statistics.metric.MetricCollection.MetricCollectionBuilder;
import wres.engine.statistics.metric.RankHistogram.RankHistogramBuilder;
import wres.engine.statistics.metric.RelativeOperatingCharacteristicDiagram.RelativeOperatingCharacteristicBuilder;
import wres.engine.statistics.metric.RelativeOperatingCharacteristicScore.RelativeOperatingCharacteristicScoreBuilder;
import wres.engine.statistics.metric.ReliabilityDiagram.ReliabilityDiagramBuilder;
import wres.engine.statistics.metric.SampleSize.SampleSizeBuilder;
import wres.engine.statistics.metric.parameters.MetricParameter;

/**
 * <p>
 * A factory class for constructing metrics.
 * </p>
 * <p>
 * TODO: support construction with parameters by first defining a setParameters(EnumMap mapping) in the
 * {@link MetricBuilder} and then adding parametric methods here. The EnumMap should map an Enum of parameter
 * identifiers to {@link MetricParameter}.
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
     * Returns an instance of a {@link MetricProcessor} for processing {@link MetricInput}. Optionally, retain and merge
     * the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessor}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public MetricProcessorByLeadTime getMetricProcessorByLeadTime( final ProjectConfig config,
                                                                   final MetricOutputGroup... mergeList )
            throws MetricConfigurationException
    {
        return getMetricProcessorByLeadTime( config, null, null, mergeList );
    }

    /**
     * Returns an instance of a {@link MetricProcessor} for processing {@link MetricInput}. Optionally, retain and merge
     * the results associated with specific {@link MetricOutputGroup} across successive calls to
     * {@link MetricProcessor#apply(Object)}. If results are retained and merged across calls, the
     * {@link MetricProcessor#apply(Object)} will return the merged results from all prior calls.
     * 
     * @param config the project configuration
     * @param thresholdExecutor an optional {@link ExecutorService} for executing thresholds. Defaults to the 
     *            {@link ForkJoinPool#commonPool()}
     * @param metricExecutor an optional {@link ExecutorService} for executing metrics. Defaults to the 
     *            {@link ForkJoinPool#commonPool()} 
     * @param mergeList an optional list of {@link MetricOutputGroup} for which results should be retained and merged
     * @return the {@link MetricProcessor}
     * @throws MetricConfigurationException if the metrics are configured incorrectly
     */

    public MetricProcessorByLeadTime getMetricProcessorByLeadTime( final ProjectConfig config,
                                                                   final ExecutorService thresholdExecutor,
                                                                   final ExecutorService metricExecutor,
                                                                   final MetricOutputGroup... mergeList )
            throws MetricConfigurationException
    {
        switch ( MetricProcessor.getInputType( config ) )
        {
            case SINGLE_VALUED:
                return new MetricProcessorSingleValuedPairsByLeadTime( outputFactory,
                                                                       config,
                                                                       thresholdExecutor,
                                                                       metricExecutor,
                                                                       mergeList );
            case ENSEMBLE:
                return new MetricProcessorEnsemblePairsByLeadTime( outputFactory,
                                                                   config,
                                                                   thresholdExecutor,
                                                                   metricExecutor,
                                                                   mergeList );
            default:
                throw new UnsupportedOperationException( "Unsupported input type in the project configuration '"
                                                         + config
                                                         + "'" );
        }
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<SingleValuedPairs, ScalarOutput> ofSingleValuedScalarCollection( MetricConstants... metric )
    {
        return ofSingleValuedScalarCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link VectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<SingleValuedPairs, VectorOutput> ofSingleValuedVectorCollection( MetricConstants... metric )
    {
        return ofSingleValuedVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<SingleValuedPairs, MultiVectorOutput>
            ofSingleValuedMultiVectorCollection( MetricConstants... metric )
    {
        return ofSingleValuedMultiVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link VectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<DiscreteProbabilityPairs, VectorOutput>
            ofDiscreteProbabilityVectorCollection( MetricConstants... metric )
    {
        return ofDiscreteProbabilityVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DichotomousPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<DichotomousPairs, ScalarOutput> ofDichotomousScalarCollection( MetricConstants... metric )
    {
        return ofDichotomousScalarCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link DiscreteProbabilityPairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput>
            ofDiscreteProbabilityMultiVectorCollection( MetricConstants... metric )
    {
        return ofDiscreteProbabilityMultiVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link MulticategoryPairs} and produce
     * {@link MatrixOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<MulticategoryPairs, MatrixOutput>
            ofMulticategoryMatrixCollection( MetricConstants... metric )
    {
        return ofMulticategoryMatrixCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<EnsemblePairs, ScalarOutput> ofEnsembleScalarCollection( MetricConstants... metric )
    {
        return ofEnsembleScalarCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link VectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<EnsemblePairs, VectorOutput> ofEnsembleVectorCollection( MetricConstants... metric )
    {
        return ofEnsembleVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link EnsemblePairs} and produce
     * {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<EnsemblePairs, MultiVectorOutput>
            ofEnsembleMultiVectorCollection( MetricConstants... metric )
    {
        return ofEnsembleMultiVectorCollection( null, metric );
    }

    /**
     * Returns a {@link MetricCollection} of metrics that consume {@link SingleValuedPairs} and produce
     * {@link ScalarOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<SingleValuedPairs, ScalarOutput> ofSingleValuedScalarCollection( ExecutorService executor,
                                                                                             MetricConstants... metric )
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
     * {@link VectorOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<SingleValuedPairs, VectorOutput> ofSingleValuedVectorCollection( ExecutorService executor,
                                                                                             MetricConstants... metric )
    {
        final MetricCollectionBuilder<SingleValuedPairs, VectorOutput> builder = MetricCollectionBuilder.of();
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
     */

    public MetricCollection<SingleValuedPairs, MultiVectorOutput>
            ofSingleValuedMultiVectorCollection( ExecutorService executor,
                                                 MetricConstants... metric )
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
     * {@link VectorOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<DiscreteProbabilityPairs, VectorOutput>
            ofDiscreteProbabilityVectorCollection( ExecutorService executor,
                                                   MetricConstants... metric )
    {
        final MetricCollectionBuilder<DiscreteProbabilityPairs, VectorOutput> builder = MetricCollectionBuilder.of();
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
     */

    public MetricCollection<DichotomousPairs, ScalarOutput> ofDichotomousScalarCollection( ExecutorService executor,
                                                                                           MetricConstants... metric )
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
     */

    public MetricCollection<DiscreteProbabilityPairs, MultiVectorOutput>
            ofDiscreteProbabilityMultiVectorCollection( ExecutorService executor,
                                                        MetricConstants... metric )
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
     */

    public MetricCollection<MulticategoryPairs, MatrixOutput> ofMulticategoryMatrixCollection( ExecutorService executor,
                                                                                               MetricConstants... metric )
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
     */

    public MetricCollection<EnsemblePairs, ScalarOutput> ofEnsembleScalarCollection( ExecutorService executor,
                                                                                     MetricConstants... metric )
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
     * {@link VectorOutput}.
     * 
     * @param executor an optional {@link ExecutorService} for executing the metrics
     * @param metric the metric identifiers
     * @return a collection of metrics
     */

    public MetricCollection<EnsemblePairs, VectorOutput> ofEnsembleVectorCollection( ExecutorService executor,
                                                                                     MetricConstants... metric )
    {
        final MetricCollectionBuilder<EnsemblePairs, VectorOutput> builder = MetricCollectionBuilder.of();
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
     */

    public MetricCollection<EnsemblePairs, MultiVectorOutput> ofEnsembleMultiVectorCollection( ExecutorService executor,
                                                                                               MetricConstants... metric )
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
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link ScalarOutput}.
     * 
     * @param metric the metric identifier
     * @return the metric
     */

    public Metric<SingleValuedPairs, ScalarOutput> ofSingleValuedScalar( MetricConstants metric )
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
            case CORRELATION_PEARSONS:
                return ofCorrelationPearsons();
            case COEFFICIENT_OF_DETERMINATION:
                return ofCoefficientOfDetermination();
            case SAMPLE_SIZE:
                return ofSampleSize();
            case INDEX_OF_AGREEMENT:
                return ofIndexOfAgreement();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link SingleValuedPairs} and produces {@link VectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<SingleValuedPairs, VectorOutput> ofSingleValuedVector( MetricConstants metric )
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
     */

    public Metric<SingleValuedPairs, MultiVectorOutput> ofSingleValuedMultiVector( MetricConstants metric )
    {
        switch ( metric )
        {
            case QUANTILE_QUANTILE_DIAGRAM:
                return ofQuantileQuantileDiagram();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DiscreteProbabilityPairs} and produces {@link VectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<DiscreteProbabilityPairs, VectorOutput> ofDiscreteProbabilityVector( MetricConstants metric )
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
     */

    public Metric<DichotomousPairs, ScalarOutput> ofDichotomousScalar( MetricConstants metric )
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
     */

    public Metric<MulticategoryPairs, ScalarOutput> ofMulticategoryScalar( MetricConstants metric )
    {
        switch ( metric )
        {
            case PEIRCE_SKILL_SCORE:
                return ofPeirceSkillScoreMulti();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link DiscreteProbabilityPairs} and produces {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<DiscreteProbabilityPairs, MultiVectorOutput>
            ofDiscreteProbabilityMultiVector( MetricConstants metric )
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
     */

    public Metric<MulticategoryPairs, MatrixOutput> ofMulticategoryMatrix( MetricConstants metric )
    {
        switch ( metric )
        {
            case CONTINGENCY_TABLE:
                return ofContingencyTable();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link ScalarOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<EnsemblePairs, ScalarOutput> ofEnsembleScalar( MetricConstants metric )
    {
        switch ( metric )
        {
            case SAMPLE_SIZE:
                return ofSampleSize();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link VectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<EnsemblePairs, VectorOutput> ofEnsembleVector( MetricConstants metric )
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
     * Returns a {@link Metric} that consumes {@link EnsemblePairs} and produces {@link MultiVectorOutput}.
     * 
     * @param metric the metric identifier
     * @return a metric
     */

    public Metric<EnsemblePairs, MultiVectorOutput> ofEnsembleMultiVector( MetricConstants metric )
    {
        switch ( metric )
        {
            case RANK_HISTOGRAM:
                return ofRankHistogram();
            default:
                throw new IllegalArgumentException( error + " '" + metric + "'." );
        }
    }

    /**
     * Return a default {@link BiasFraction} function.
     * 
     * @return a default {@link BiasFraction} function
     */

    BiasFraction ofBiasFraction()
    {
        return (BiasFraction) new BiasFraction.BiasFractionBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link BrierScore} function.
     * 
     * @return a default {@link BrierScore} function
     */

    BrierScore ofBrierScore()
    {
        return (BrierScore) new BrierScore.BrierScoreBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link BrierSkillScore} function.
     * 
     * @return a default {@link BrierSkillScore} function
     */

    BrierSkillScore ofBrierSkillScore()
    {
        return (BrierSkillScore) new BrierSkillScore.BrierSkillScoreBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link CoefficientOfDetermination} function.
     * 
     * @return a default {@link CoefficientOfDetermination} function
     */

    CorrelationPearsons ofCoefficientOfDetermination()
    {
        return (CoefficientOfDetermination) new CoefficientOfDetermination.CoefficientOfDeterminationBuilder().setOutputFactory( outputFactory )
                                                                                                              .build();
    }

    /**
     * Return a default {@link ContingencyTable} function.
     * 
     * @return a default {@link ContingencyTable} function
     */

    ContingencyTable<MulticategoryPairs> ofContingencyTable()
    {
        return (ContingencyTable<MulticategoryPairs>) new ContingencyTable.ContingencyTableBuilder<>().setOutputFactory( outputFactory )
                                                                                                      .build();
    }

    /**
     * Return a default {@link CorrelationPearsons} function.
     * 
     * @return a default {@link CorrelationPearsons} function
     */

    CorrelationPearsons ofCorrelationPearsons()
    {
        return (CorrelationPearsons) new CorrelationPearsons.CorrelationPearsonsBuilder().setOutputFactory( outputFactory )
                                                                                         .build();
    }

    /**
     * Return a default {@link CriticalSuccessIndex} function.
     * 
     * @return a default {@link CriticalSuccessIndex} function
     */

    CriticalSuccessIndex ofCriticalSuccessIndex()
    {
        return (CriticalSuccessIndex) new CriticalSuccessIndex.CriticalSuccessIndexBuilder().setOutputFactory( outputFactory )
                                                                                            .build();
    }

    /**
     * Return a default {@link EquitableThreatScore} function.
     * 
     * @return a default {@link EquitableThreatScore} function
     */

    EquitableThreatScore ofEquitableThreatScore()
    {
        return (EquitableThreatScore) new EquitableThreatScore.EquitableThreatScoreBuilder().setOutputFactory( outputFactory )
                                                                                            .build();
    }

    /**
     * Return a default {@link MeanAbsoluteError} function.
     * 
     * @return a default {@link MeanAbsoluteError} function
     */

    MeanAbsoluteError ofMeanAbsoluteError()
    {
        return (MeanAbsoluteError) new MeanAbsoluteError.MeanAbsoluteErrorBuilder().setOutputFactory( outputFactory )
                                                                                   .build();
    }

    /**
     * Return a default {@link MeanError} function.
     * 
     * @return a default {@link MeanError} function
     */

    MeanError ofMeanError()
    {
        return (MeanError) new MeanError.MeanErrorBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link MeanSquareError} function.
     * 
     * @return a default {@link MeanSquareError} function
     */

    MeanSquareError<SingleValuedPairs> ofMeanSquareError()
    {
        return (MeanSquareError<SingleValuedPairs>) new MeanSquareError.MeanSquareErrorBuilder<>().setOutputFactory( outputFactory )
                                                                                                  .build();
    }

    /**
     * Return a default {@link MeanSquareErrorSkillScore} function.
     * 
     * @return a default {@link MeanSquareErrorSkillScore} function
     */

    MeanSquareErrorSkillScore<SingleValuedPairs> ofMeanSquareErrorSkillScore()
    {
        return (MeanSquareErrorSkillScore<SingleValuedPairs>) new MeanSquareErrorSkillScore.MeanSquareErrorSkillScoreBuilder<>().setOutputFactory( outputFactory )
                                                                                                                                .build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a dichotomous event.
     * 
     * @return a default {@link PeirceSkillScore} function for a dichotomous event
     */

    PeirceSkillScore<DichotomousPairs> ofPeirceSkillScore()
    {
        return (PeirceSkillScore<DichotomousPairs>) new PeirceSkillScore.PeirceSkillScoreBuilder<DichotomousPairs>().setOutputFactory( outputFactory )
                                                                                                                    .build();
    }

    /**
     * Return a default {@link PeirceSkillScore} function for a multicategory event.
     * 
     * @return a default {@link PeirceSkillScore} function for a multicategory event
     */

    PeirceSkillScore<MulticategoryPairs> ofPeirceSkillScoreMulti()
    {
        return (PeirceSkillScore<MulticategoryPairs>) new PeirceSkillScore.PeirceSkillScoreBuilder<MulticategoryPairs>().setOutputFactory( outputFactory )
                                                                                                                        .build();
    }

    /**
     * Return a default {@link ProbabilityOfDetection} function.
     * 
     * @return a default {@link ProbabilityOfDetection} function
     */

    ProbabilityOfDetection ofProbabilityOfDetection()
    {
        return (ProbabilityOfDetection) new ProbabilityOfDetection.ProbabilityOfDetectionBuilder().setOutputFactory( outputFactory )
                                                                                                  .build();
    }

    /**
     * Return a default {@link ProbabilityOfFalseDetection} function.
     * 
     * @return a default {@link ProbabilityOfFalseDetection} function
     */

    ProbabilityOfFalseDetection ofProbabilityOfFalseDetection()
    {
        return (ProbabilityOfFalseDetection) new ProbabilityOfFalseDetection.ProbabilityOfFalseDetectionBuilder().setOutputFactory( outputFactory )
                                                                                                                 .build();
    }

    /**
     * Return a default {@link QuantileQuantileDiagram} function.
     * 
     * @return a default {@link QuantileQuantileDiagram} function
     */

    QuantileQuantileDiagram ofQuantileQuantileDiagram()
    {
        return (QuantileQuantileDiagram) new QuantileQuantileDiagram.QuantileQuantileDiagramBuilder().setOutputFactory( outputFactory )
                                                                                                     .build();
    }

    /**
     * Return a default {@link RootMeanSquareError} function.
     * 
     * @return a default {@link RootMeanSquareError} function
     */

    RootMeanSquareError ofRootMeanSquareError()
    {
        return (RootMeanSquareError) new RootMeanSquareError.RootMeanSquareErrorBuilder().setOutputFactory( outputFactory )
                                                                                         .build();
    }

    /**
     * Return a default {@link ReliabilityDiagram} function.
     * 
     * @return a default {@link ReliabilityDiagram} function
     */

    ReliabilityDiagram ofReliabilityDiagram()
    {
        return (ReliabilityDiagram) new ReliabilityDiagramBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link RelativeOperatingCharacteristicDiagram} function.
     * 
     * @return a default {@link RelativeOperatingCharacteristicDiagram} function
     */

    RelativeOperatingCharacteristicDiagram ofRelativeOperatingCharacteristic()
    {
        return (RelativeOperatingCharacteristicDiagram) new RelativeOperatingCharacteristicBuilder().setOutputFactory( outputFactory )
                                                                                                    .build();
    }

    /**
     * Return a default {@link RelativeOperatingCharacteristicScore} function.
     * 
     * @return a default {@link RelativeOperatingCharacteristicScore} function
     */

    RelativeOperatingCharacteristicScore ofRelativeOperatingCharacteristicScore()
    {
        return (RelativeOperatingCharacteristicScore) new RelativeOperatingCharacteristicScoreBuilder().setOutputFactory( outputFactory )
                                                                                                       .build();
    }

    /**
     * Return a default {@link ContinuousRankedProbabilityScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilityScore} function
     */

    ContinuousRankedProbabilityScore ofContinuousRankedProbabilityScore()
    {
        return (ContinuousRankedProbabilityScore) new CRPSBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link ContinuousRankedProbabilitySkillScore} function.
     * 
     * @return a default {@link ContinuousRankedProbabilitySkillScore} function
     */

    ContinuousRankedProbabilityScore ofContinuousRankedProbabilitySkillScore()
    {
        return (ContinuousRankedProbabilitySkillScore) new CRPSSBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link IndexOfAgreement} function.
     * 
     * @return a default {@link IndexOfAgreement} function
     */

    IndexOfAgreement ofIndexOfAgreement()
    {
        return (IndexOfAgreement) new IndexOfAgreementBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link KlingGuptaEfficiency} function.
     * 
     * @return a default {@link KlingGuptaEfficiency} function
     */

    KlingGuptaEfficiency ofKlingGuptaEfficiency()
    {
        return (KlingGuptaEfficiency) new KlingGuptaEfficiencyBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link SampleSize} function.
     * 
     * @param <T> the type of {@link MetricInput}
     * @return a default {@link SampleSize} function
     */

    <T extends MetricInput<?>> SampleSize<T> ofSampleSize()
    {
        return (SampleSize<T>) new SampleSizeBuilder<T>().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link RankHistogram} function.
     * 
     * @return a default {@link RankHistogram} function
     */

    RankHistogram ofRankHistogram()
    {
        return (RankHistogram) new RankHistogramBuilder().setOutputFactory( outputFactory ).build();
    }

    /**
     * Return a default {@link FrequencyBias} function.
     * 
     * @return a default {@link FrequencyBias} function
     */

    FrequencyBias ofFrequencyBias()
    {
        return (FrequencyBias) new FrequencyBiasBuilder().setOutputFactory( outputFactory ).build();
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
