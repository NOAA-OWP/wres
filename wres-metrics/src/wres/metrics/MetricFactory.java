package wres.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.types.Ensemble;
import wres.datamodel.pools.Pool;
import wres.datamodel.types.Probability;
import wres.config.MetricConstants;
import wres.config.MetricConstants.SampleDataGroup;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.metrics.MetricCollection.Builder;
import wres.metrics.categorical.ContingencyTable;
import wres.metrics.categorical.EquitableThreatScore;
import wres.metrics.categorical.FalseAlarmRatio;
import wres.metrics.categorical.FrequencyBias;
import wres.metrics.categorical.PeirceSkillScore;
import wres.metrics.categorical.ProbabilityOfDetection;
import wres.metrics.categorical.ProbabilityOfFalseDetection;
import wres.metrics.categorical.ThreatScore;
import wres.metrics.discreteprobability.BrierScore;
import wres.metrics.discreteprobability.BrierSkillScore;
import wres.metrics.discreteprobability.RelativeOperatingCharacteristicDiagram;
import wres.metrics.discreteprobability.RelativeOperatingCharacteristicScore;
import wres.metrics.discreteprobability.ReliabilityDiagram;
import wres.metrics.ensemble.BoxPlotErrorByForecast;
import wres.metrics.ensemble.BoxPlotErrorByObserved;
import wres.metrics.ensemble.ContinuousRankedProbabilityScore;
import wres.metrics.ensemble.ContinuousRankedProbabilitySkillScore;
import wres.metrics.ensemble.EnsembleQuantileQuantileDiagram;
import wres.metrics.ensemble.RankHistogram;
import wres.metrics.singlevalued.BiasFraction;
import wres.metrics.singlevalued.BoxPlotError;
import wres.metrics.singlevalued.BoxPlotPercentageError;
import wres.metrics.singlevalued.CoefficientOfDetermination;
import wres.metrics.singlevalued.CorrelationPearsons;
import wres.metrics.singlevalued.IndexOfAgreement;
import wres.metrics.singlevalued.KlingGuptaEfficiency;
import wres.metrics.singlevalued.MeanAbsoluteError;
import wres.metrics.singlevalued.MeanAbsoluteErrorSkillScore;
import wres.metrics.singlevalued.MeanError;
import wres.metrics.singlevalued.MeanSquareError;
import wres.metrics.singlevalued.MeanSquareErrorSkillScore;
import wres.metrics.singlevalued.MeanSquareErrorSkillScoreNormalized;
import wres.metrics.singlevalued.MedianError;
import wres.metrics.singlevalued.QuantileQuantileDiagram;
import wres.metrics.singlevalued.RootMeanSquareError;
import wres.metrics.singlevalued.RootMeanSquareErrorNormalized;
import wres.metrics.singlevalued.SumOfSquareError;
import wres.metrics.singlevalued.VolumetricEfficiency;
import wres.metrics.singlevalued.univariate.Maximum;
import wres.metrics.singlevalued.univariate.Mean;
import wres.metrics.singlevalued.univariate.Minimum;
import wres.metrics.singlevalued.univariate.StandardDeviation;
import wres.metrics.timeseries.TimeToPeakError;
import wres.metrics.timeseries.TimeToPeakRelativeError;
import wres.metrics.timeseries.TimingErrorDurationStatistics;

/**
 * <p>A factory class for constructing metrics.
 *
 * @author James Brown
 */

public final class MetricFactory
{
    /** String used in several error messages to denote an unrecognized metric. */
    private static final String UNRECOGNIZED_METRIC_ERROR = "Unrecognized metric for identifier.";

    /** Test seed system property name. */
    private static final String TEST_SEED_PROPERTY = "wres.systemTestSeed";

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

    public static MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
    ofSingleValuedScores( MetricConstants... metric )
    {
        return MetricFactory.ofSingleValuedScores( ForkJoinPool.commonPool(), metric );
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
    ofSingleValuedDiagrams( MetricConstants... metric )
    {
        return MetricFactory.ofSingleValuedDiagrams( ForkJoinPool.commonPool(), metric );
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

    public static MetricCollection<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
    ofDiscreteProbabilityScores( MetricConstants... metric )
    {
        return MetricFactory.ofDiscreteProbabilityScores( ForkJoinPool.commonPool(), metric );
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

    public static MetricCollection<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
    ofDichotomousScores( MetricConstants... metric )
    {
        return MetricFactory.ofDichotomousScores( ForkJoinPool.commonPool(), metric );
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

    public static MetricCollection<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter, DiagramStatisticOuter>
    ofDiscreteProbabilityDiagrams( MetricConstants... metric )
    {
        return MetricFactory.ofDiscreteProbabilityDiagrams( ForkJoinPool.commonPool(), metric );
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

    public static MetricCollection<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
    ofEnsembleScores( MetricConstants... metric )
    {
        return MetricFactory.ofEnsembleScores( ForkJoinPool.commonPool(), metric );
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
    ofEnsembleDiagrams( MetricConstants... metric )
    {
        return MetricFactory.ofEnsembleDiagrams( ForkJoinPool.commonPool(), metric );
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
    ofEnsembleBoxplots( MetricConstants... metric )
    {
        return MetricFactory.ofEnsembleBoxplots( ForkJoinPool.commonPool(), metric );
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

    public static MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter>
    ofSingleValuedTimeSeriesMetrics( MetricConstants... metric )
    {
        return MetricFactory.ofSingleValuedTimeSeriesMetrics( ForkJoinPool.commonPool(), metric );
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

    public static MetricCollection<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
    ofSingleValuedScores( ExecutorService executor,
                          MetricConstants... metric )
    {
        Builder<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> builder =
                Builder.of();

        // Add the metrics
        for ( MetricConstants next : metric )
        {
            Metric<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter> m = MetricFactory.ofSingleValuedScore( next );
            if ( MetricFactory.isCollectable( next ) )
            {
                Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> c =
                        ( Collectable<Pool<Pair<Double, Double>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> ) m;
                builder.addCollectableMetric( c );
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
    ofSingleValuedDiagrams( ExecutorService executor,
                            MetricConstants... metric )
    {
        Builder<Pool<Pair<Double, Double>>, DiagramStatisticOuter, DiagramStatisticOuter> builder =
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
    ofSingleValuedBoxplots( ExecutorService executor,
                            MetricConstants... metric )
    {
        Builder<Pool<Pair<Double, Double>>, BoxplotStatisticOuter, BoxplotStatisticOuter> builder =
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

    public static MetricCollection<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
    ofDiscreteProbabilityScores( ExecutorService executor,
                                 MetricConstants... metric )
    {
        Builder<Pool<Pair<Probability, Probability>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
                builder =
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

    public static MetricCollection<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
    ofDichotomousScores( ExecutorService executor,
                         MetricConstants... metric )
    {
        Builder<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> builder =
                Builder.of();
        for ( MetricConstants next : metric )
        {
            Metric<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter> m =
                    MetricFactory.ofDichotomousScore( next );
            if ( MetricFactory.isCollectable( next ) )
            {
                Collectable<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> c =
                        ( Collectable<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> ) m;
                builder.addCollectableMetric( c );
            }
            else
            {
                builder.addMetric( m );
            }
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

    public static MetricCollection<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter, DiagramStatisticOuter>
    ofDiscreteProbabilityDiagrams( ExecutorService executor,
                                   MetricConstants... metric )
    {
        Builder<Pool<Pair<Probability, Probability>>, DiagramStatisticOuter, DiagramStatisticOuter> builder =
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

    public static MetricCollection<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter>
    ofEnsembleScores( ExecutorService executor,
                      MetricConstants... metric )
    {
        Builder<Pool<Pair<Double, Ensemble>>, DoubleScoreStatisticOuter, DoubleScoreStatisticOuter> builder =
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
    ofEnsembleDiagrams( ExecutorService executor,
                        MetricConstants... metric )
    {
        Builder<Pool<Pair<Double, Ensemble>>, DiagramStatisticOuter, DiagramStatisticOuter> builder =
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
    ofEnsembleBoxplots( ExecutorService executor,
                        MetricConstants... metric )
    {
        Builder<Pool<Pair<Double, Ensemble>>, BoxplotStatisticOuter, BoxplotStatisticOuter> builder =
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

    public static MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter>
    ofSingleValuedTimeSeriesMetrics( ExecutorService executor,
                                     MetricConstants... metric )
    {
        Builder<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter>
                builder =
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
        return switch ( metric )
        {
            case BIAS_FRACTION -> BiasFraction.of();
            case BIAS_FRACTION_DIFFERENCE -> DoubleScoreDifference.of( BiasFraction.of() );
            case KLING_GUPTA_EFFICIENCY -> KlingGuptaEfficiency.of();
            case KLING_GUPTA_EFFICIENCY_DIFFERENCE -> DoubleScoreDifference.of( KlingGuptaEfficiency.of() );
            case MEAN_ABSOLUTE_ERROR -> MeanAbsoluteError.of();
            case MEAN_ABSOLUTE_ERROR_DIFFERENCE -> DoubleScoreDifference.of( MeanAbsoluteError.of() );
            case MEAN_ABSOLUTE_ERROR_SKILL_SCORE -> MeanAbsoluteErrorSkillScore.of();
            case MEAN_ERROR -> MeanError.of();
            case MEAN_ERROR_DIFFERENCE -> DoubleScoreDifference.of( MeanError.of() );
            case MEDIAN_ERROR -> MedianError.of();
            case MEDIAN_ERROR_DIFFERENCE -> DoubleScoreDifference.of( MedianError.of() );
            case SAMPLE_SIZE -> SampleSize.of();
            case SAMPLE_SIZE_DIFFERENCE -> DoubleScoreDifference.of( SampleSize.of() );
            case INDEX_OF_AGREEMENT -> IndexOfAgreement.of();
            case INDEX_OF_AGREEMENT_DIFFERENCE -> DoubleScoreDifference.of( IndexOfAgreement.of() );
            case VOLUMETRIC_EFFICIENCY -> VolumetricEfficiency.of();
            case VOLUMETRIC_EFFICIENCY_DIFFERENCE -> DoubleScoreDifference.of( VolumetricEfficiency.of() );
            case MEAN_SQUARE_ERROR_SKILL_SCORE -> MeanSquareErrorSkillScore.of();
            case MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED -> MeanSquareErrorSkillScoreNormalized.of();
            case COEFFICIENT_OF_DETERMINATION -> CoefficientOfDetermination.of();
            case COEFFICIENT_OF_DETERMINATION_DIFFERENCE -> DoubleScoreDifference.of( CoefficientOfDetermination.of() );
            case PEARSON_CORRELATION_COEFFICIENT -> CorrelationPearsons.of();
            case PEARSON_CORRELATION_COEFFICIENT_DIFFERENCE -> DoubleScoreDifference.of( CorrelationPearsons.of() );
            case MEAN_SQUARE_ERROR -> MeanSquareError.of();
            case MEAN_SQUARE_ERROR_DIFFERENCE -> DoubleScoreDifference.of( MeanSquareError.of() );
            case ROOT_MEAN_SQUARE_ERROR -> RootMeanSquareError.of();
            case ROOT_MEAN_SQUARE_ERROR_DIFFERENCE -> DoubleScoreDifference.of( RootMeanSquareError.of() );
            case ROOT_MEAN_SQUARE_ERROR_NORMALIZED -> RootMeanSquareErrorNormalized.of();
            case ROOT_MEAN_SQUARE_ERROR_NORMALIZED_DIFFERENCE ->
                    DoubleScoreDifference.of( RootMeanSquareErrorNormalized.of() );
            case SUM_OF_SQUARE_ERROR -> SumOfSquareError.of();
            case SUM_OF_SQUARE_ERROR_DIFFERENCE -> DoubleScoreDifference.of( SumOfSquareError.of() );
            case MEAN -> Mean.of();
            case STANDARD_DEVIATION -> StandardDeviation.of();
            case MINIMUM -> Minimum.of();
            case MAXIMUM -> Maximum.of();
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        };
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
        return switch ( metric )
        {
            case BOX_PLOT_OF_ERRORS -> BoxPlotError.of();
            case BOX_PLOT_OF_PERCENTAGE_ERRORS -> BoxPlotPercentageError.of();
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        };
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
        return switch ( metric )
        {
            case BRIER_SCORE -> BrierScore.of();
            case BRIER_SCORE_DIFFERENCE -> DoubleScoreDifference.of( BrierScore.of() );
            case BRIER_SKILL_SCORE -> BrierSkillScore.of();
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE -> RelativeOperatingCharacteristicScore.of();
            case RELATIVE_OPERATING_CHARACTERISTIC_SCORE_DIFFERENCE ->
                // Calculate a raw score without a baseline using the supplied parameter
                    DoubleScoreDifference.of( RelativeOperatingCharacteristicScore.of( false ) );
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        };
    }

    /**
     * Returns a {@link Metric} that consumes dichotomous pairs and produces {@link DoubleScoreStatisticOuter}.
     *
     * @param metric the metric identifier
     * @return a metric
     * @throws IllegalArgumentException if the metric identifier is not recognized
     */

    public static Metric<Pool<Pair<Boolean, Boolean>>, DoubleScoreStatisticOuter>
    ofDichotomousScore( MetricConstants metric )
    {
        return switch ( metric )
        {
            case THREAT_SCORE -> ThreatScore.of();
            case THREAT_SCORE_DIFFERENCE -> DoubleScoreDifference.of( ThreatScore.of() );
            case EQUITABLE_THREAT_SCORE -> EquitableThreatScore.of();
            case EQUITABLE_THREAT_SCORE_DIFFERENCE -> DoubleScoreDifference.of( EquitableThreatScore.of() );
            case PEIRCE_SKILL_SCORE -> PeirceSkillScore.of();
            case PEIRCE_SKILL_SCORE_DIFFERENCE -> DoubleScoreDifference.of( PeirceSkillScore.of() );
            case PROBABILITY_OF_DETECTION -> ProbabilityOfDetection.of();
            case PROBABILITY_OF_DETECTION_DIFFERENCE -> DoubleScoreDifference.of( ProbabilityOfDetection.of() );
            case PROBABILITY_OF_FALSE_DETECTION -> ProbabilityOfFalseDetection.of();
            case PROBABILITY_OF_FALSE_DETECTION_DIFFERENCE ->
                    DoubleScoreDifference.of( ProbabilityOfFalseDetection.of() );
            case FREQUENCY_BIAS -> FrequencyBias.of();
            case FREQUENCY_BIAS_DIFFERENCE -> DoubleScoreDifference.of( FrequencyBias.of() );
            case CONTINGENCY_TABLE -> ContingencyTable.of();
            case FALSE_ALARM_RATIO -> FalseAlarmRatio.of();
            case FALSE_ALARM_RATIO_DIFFERENCE -> DoubleScoreDifference.of( FalseAlarmRatio.of() );
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        };
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
        return switch ( metric )
        {
            case RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM -> RelativeOperatingCharacteristicDiagram.of();
            case RELIABILITY_DIAGRAM -> ReliabilityDiagram.of();
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        };
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
        return switch ( metric )
        {
            case CONTINUOUS_RANKED_PROBABILITY_SCORE -> ContinuousRankedProbabilityScore.of();
            case CONTINUOUS_RANKED_PROBABILITY_SCORE_DIFFERENCE ->
                    DoubleScoreDifference.of( ContinuousRankedProbabilityScore.of() );
            case CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE -> ContinuousRankedProbabilitySkillScore.of();
            case SAMPLE_SIZE -> SampleSize.of();
            case SAMPLE_SIZE_DIFFERENCE -> DoubleScoreDifference.of( SampleSize.of() );
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        };
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
        return switch ( metric )
        {
            case BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE -> BoxPlotErrorByObserved.of();
            case BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE -> BoxPlotErrorByForecast.of();
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        };
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
            case RANK_HISTOGRAM ->
            {
                Random random = MetricFactory.getRandomNumberGenerator();
                return RankHistogram.of( random );
            }
            case ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM ->
            {
                return EnsembleQuantileQuantileDiagram.of();
            }
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
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

    public static Metric<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter>
    ofSingleValuedTimeSeries( MetricConstants metric )
    {
        // Use a random number generator with a fixed seed if required
        Random random = MetricFactory.getRandomNumberGenerator();

        return switch ( metric )
        {
            case TIME_TO_PEAK_ERROR -> TimeToPeakError.of( random );
            case TIME_TO_PEAK_RELATIVE_ERROR -> TimeToPeakRelativeError.of( random );
            default -> throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + metric + "'." );
        };
    }

    /**
     * Helper that returns timing error summary statistics for a nominated type of timing error metric.
     *
     * @param timingMetric the timing error metric
     * @param summaryStatistics the set of summary statistics to compute
     * @return the summary statistics calculator
     * @throws NullPointerException if any input is null
     */

    public static Metric<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter>
    ofSummaryStatisticsForTimingErrorMetric( MetricConstants timingMetric,
                                             Set<MetricConstants> summaryStatistics )
    {
        Objects.requireNonNull( timingMetric, "Specify a non-null timing error metric." );
        Objects.requireNonNull( summaryStatistics, "Specify non-null summary statistics for the timing error metric." );


        Random random = MetricFactory.getRandomNumberGenerator();

        return switch ( timingMetric )
        {
            case TIME_TO_PEAK_ERROR ->
                    TimingErrorDurationStatistics.of( TimeToPeakError.of( random ), summaryStatistics );
            case TIME_TO_PEAK_RELATIVE_ERROR ->
                    TimingErrorDurationStatistics.of( TimeToPeakRelativeError.of( random ), summaryStatistics );
            default -> throw new IllegalArgumentException(
                    UNRECOGNIZED_METRIC_ERROR + " '" + timingMetric + "'." );
        };
    }

    /**
     * Helper that returns timing error summary statistics for a nominated type of timing error metric.
     *
     * @param executor the executor
     * @param timingMetrics the timing error metrics and associated summary statistics
     * @return the summary statistics calculators
     * @throws NullPointerException if the map of timing metrics is null
     */

    public static MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter, DurationScoreStatisticOuter>
    ofSummaryStatisticsForTimingErrorMetrics( ExecutorService executor,
                                              Map<MetricConstants, Set<MetricConstants>> timingMetrics )
    {
        Objects.requireNonNull( timingMetrics, "Specify a non-null map of timing error metrics." );

        MetricCollection.Builder<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter, DurationScoreStatisticOuter>
                builder =
                new MetricCollection.Builder<>();
        builder.setExecutorService( executor );

        for ( Map.Entry<MetricConstants, Set<MetricConstants>> nextMetric : timingMetrics.entrySet() )
        {
            MetricConstants nextTimingMetric = nextMetric.getKey();
            Set<MetricConstants> nextSummaryStatistics = nextMetric.getValue();

            Metric<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter> metric =
                    MetricFactory.ofSummaryStatisticsForTimingErrorMetric( nextTimingMetric, nextSummaryStatistics );

            builder.addMetric( metric );
        }

        return builder.build();
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

        // DoubleScoreDifference does not implement Collectable
        if ( metric.isDifferenceMetric() )
        {
            return false;
        }

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
     */

    private MetricFactory()
    {
    }

}
