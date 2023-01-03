package wres.metrics;

import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.DataUtilities;
import wres.datamodel.Ensemble;
import wres.datamodel.pools.Pool;
import wres.datamodel.Probability;
import wres.datamodel.metrics.MetricConstants;
import wres.datamodel.metrics.MetricConstants.SampleDataGroup;
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

    /**
     * String used in several error messages to denote an unrecognized metric.
     */

    private static final String UNRECOGNIZED_METRIC_ERROR = "Unrecognized metric for identifier.";

    /**
     * Test seed system property name.
     */

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
            MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter>
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
            MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter>
            ofSingleValuedTimeSeriesCollection( ExecutorService executor,
                                                MetricConstants... metric )
    {
        final Builder<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter, DurationDiagramStatisticOuter> builder =
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
            case MEAN_ABSOLUTE_ERROR_SKILL_SCORE:
                return MeanAbsoluteErrorSkillScore.of();
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
            case FALSE_ALARM_RATIO:
                return FalseAlarmRatio.of();
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

    public static Metric<Pool<TimeSeries<Pair<Double, Double>>>, DurationDiagramStatisticOuter>
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

        switch ( timingMetric )
        {
            case TIME_TO_PEAK_ERROR:
                return TimingErrorDurationStatistics.of( TimeToPeakError.of( random ), summaryStatistics );
            case TIME_TO_PEAK_RELATIVE_ERROR:
                return TimingErrorDurationStatistics.of( TimeToPeakRelativeError.of( random ), summaryStatistics );
            default:
                throw new IllegalArgumentException( UNRECOGNIZED_METRIC_ERROR + " '" + timingMetric + "'." );
        }
    }

    /**
     * Helper that returns timing error summary statistics for a nominated type of timing error metric.
     * 
     * @param executor the executor
     * @param timingMetrics the timing error metrics and associated summary statistics
     * @return the summary statistics calculators
     * @throws NullPointerException if the map of timing metrics is null
     */

    public static
            MetricCollection<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter, DurationScoreStatisticOuter>
            ofSummaryStatisticsForTimingErrorMetrics( ExecutorService executor,
                                                      Map<MetricConstants, Set<MetricConstants>> timingMetrics )
    {
        Objects.requireNonNull( timingMetrics, "Specify a non-null map of timing error metrics." );

        MetricCollection.Builder<Pool<TimeSeries<Pair<Double, Double>>>, DurationScoreStatisticOuter, DurationScoreStatisticOuter> builder =
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
     * @param outputFactory a {@link DataUtilities}
     */

    private MetricFactory()
    {
    }

}
