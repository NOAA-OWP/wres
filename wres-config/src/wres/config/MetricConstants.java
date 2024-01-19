package wres.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.MetricName;

/**
 * Metric constants. The metric identifiers are grouped by metric input/output type, as defined by the
 * {@link SampleDataGroup} and {@link StatisticType}, respectively.
 *
 * @author James Brown
 */

public enum MetricConstants
{
    /** Fractional bias or relative mean error. */
    BIAS_FRACTION( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                   new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Brier Score. */
    BRIER_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE,
                 new Limits( 0, 1, 0 ) ),

    /** Brier Skill Score. */
    BRIER_SKILL_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE, true,
                       new Limits( Double.NEGATIVE_INFINITY, 1, 1 ) ),

    /** Box plot of errors by observed value. */
    BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE( SampleDataGroup.ENSEMBLE, StatisticType.BOXPLOT_PER_PAIR ),

    /** Box plot of errors by forecast value. */
    BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE( SampleDataGroup.ENSEMBLE, StatisticType.BOXPLOT_PER_PAIR ),

    /** Box plot of errors. */
    BOX_PLOT_OF_ERRORS( SampleDataGroup.SINGLE_VALUED, StatisticType.BOXPLOT_PER_POOL ),

    /** Box plot of errors as a percentage of the left value.*/
    BOX_PLOT_OF_PERCENTAGE_ERRORS( SampleDataGroup.SINGLE_VALUED, StatisticType.BOXPLOT_PER_POOL ),

    /** Coefficient of determination.*/
    COEFFICIENT_OF_DETERMINATION( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                                  new Limits( 0, 1, 1 ) ),

    /** Mean Continuous Ranked Probability Score. */
    CONTINUOUS_RANKED_PROBABILITY_SCORE( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE,
                                         new Limits( 0, 1, 0 ) ),

    /** Mean Continuous Ranked Probability Skill Score. */
    CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE, true,
                                               new Limits( Double.NEGATIVE_INFINITY, 1, 1 ) ),

    /** Contingency Table.*/
    CONTINGENCY_TABLE( new SampleDataGroup[] { SampleDataGroup.DICHOTOMOUS,
            SampleDataGroup.MULTICATEGORY },
                       StatisticType.DOUBLE_SCORE, false,
                       new Limits( 0, Double.POSITIVE_INFINITY, Double.NaN ),
                       MetricGroup.CONTINGENCY_TABLE ),

    /** False alarm ratio.*/
    FALSE_ALARM_RATIO( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE,
                       new Limits( 0, 1, 0 ) ),

    /** Pearson's product-moment correlation coefficient.*/
    PEARSON_CORRELATION_COEFFICIENT( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                                     new Limits( -1, 1, 1 ) ),

    /** Threat Score. */
    THREAT_SCORE( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE,
                  new Limits( 0, 1, 1 ) ),

    /** Equitable Threat Score.*/
    EQUITABLE_THREAT_SCORE( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, true,
                            new Limits( -1.0 / 3, Double.POSITIVE_INFINITY, 1 ) ),

    /** Frequency Bias.*/
    FREQUENCY_BIAS( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE,
                    new Limits( 0, Double.POSITIVE_INFINITY, 1 ) ),

    /** Index of Agreement. */
    INDEX_OF_AGREEMENT( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                        new Limits( 0, 1, 1 ) ),

    /** Kling-Gupta Efficiency index. */
    KLING_GUPTA_EFFICIENCY( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, true,
                            new Limits( Double.NEGATIVE_INFINITY, 1, 1 ) ),

    /** Mean Absolute Error.*/
    MEAN_ABSOLUTE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                         new Limits( 0, Double.POSITIVE_INFINITY, 0 ) ),

    /** Mean Absolute Error Skill Score.*/
    MEAN_ABSOLUTE_ERROR_SKILL_SCORE( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, true,
                                     new Limits( Double.NEGATIVE_INFINITY, 1, 1 ) ),

    /** Mean Error. */
    MEAN_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Mean Square Error.*/
    MEAN_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                       new Limits( 0, Double.POSITIVE_INFINITY, 0 ) ),

    /** Mean Square Error Skill Score.*/
    MEAN_SQUARE_ERROR_SKILL_SCORE( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, true,
                                   new Limits( Double.NEGATIVE_INFINITY, 1, 1 ) ),

    /** Mean Square Error Skill Score, normalized. */
    MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, true,
                                              new Limits( 0, 1, 1 ) ),

    /** Median Error.*/
    MEDIAN_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                  new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Peirce Skill Score. */
    PEIRCE_SKILL_SCORE( new SampleDataGroup[] { SampleDataGroup.DICHOTOMOUS, SampleDataGroup.MULTICATEGORY },
                        StatisticType.DOUBLE_SCORE, true,
                        new Limits( -1, 1, 1 ) ),

    /** Probability Of Detection. */
    PROBABILITY_OF_DETECTION( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE,
                              new Limits( 0, 1, 1 ) ),

    /** Probability Of False Detection.*/
    PROBABILITY_OF_FALSE_DETECTION( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE,
                                    new Limits( 0, 1, 0 ) ),

    /** Quantile-quantile diagram. */
    QUANTILE_QUANTILE_DIAGRAM( SampleDataGroup.SINGLE_VALUED, StatisticType.DIAGRAM,
                               new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN ) ),

    /** Ensemble Quantile-quantile diagram. */
    ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM( SampleDataGroup.ENSEMBLE, StatisticType.DIAGRAM,
                                        new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN ) ),

    /** Rank Histogram. */
    RANK_HISTOGRAM( SampleDataGroup.ENSEMBLE, StatisticType.DIAGRAM,
                    new Limits( 0, Double.POSITIVE_INFINITY, Double.NaN ) ),

    /** Relative Operating Characteristic. */
    RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DIAGRAM,
                                               new Limits( 0, 1, Double.NaN ) ),

    /** Relative Operating Characteristic Score. */
    RELATIVE_OPERATING_CHARACTERISTIC_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE, true,
                                             new Limits( 0, 1, 1 ) ),

    /** Reliability Diagram. */
    RELIABILITY_DIAGRAM( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DIAGRAM,
                         new Limits( 0, 1, Double.NaN ) ),

    /** Root Mean Square Error. */
    ROOT_MEAN_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                            new Limits( 0, Double.POSITIVE_INFINITY, 0 ) ),

    /** Root Mean Square Error normalized by the standard deviation of the left values. */
    ROOT_MEAN_SQUARE_ERROR_NORMALIZED( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                                       new Limits( 0, Double.POSITIVE_INFINITY, 0 ) ),

    /** Sample size. */
    SAMPLE_SIZE( new SampleDataGroup[] { SampleDataGroup.SINGLE_VALUED,
            SampleDataGroup.ENSEMBLE },
                 StatisticType.DOUBLE_SCORE, false,
                 new Limits( 0, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY ),
                 MetricGroup.UNIVARIATE_STATISTIC ),

    /** Sum of Square Error. */
    SUM_OF_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                         new Limits( 0, Double.POSITIVE_INFINITY, 0 ) ),

    /** Volumetric Efficiency. */
    VOLUMETRIC_EFFICIENCY( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                           new Limits( Double.NEGATIVE_INFINITY, 1, 1 ) ),

    /** Time-to-Peak Error. */
    TIME_TO_PEAK_ERROR( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_DIAGRAM ),

    /** A statistic derived from the {@link #TIME_TO_PEAK_ERROR}. */
    TIME_TO_PEAK_ERROR_STATISTIC( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_SCORE ),

    /** Time-to-Peak Relative Error. */
    TIME_TO_PEAK_RELATIVE_ERROR( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_DIAGRAM ),

    /** A statistic derived from the {@link #TIME_TO_PEAK_RELATIVE_ERROR}. */
    TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_SCORE ),

    /** Mean statistic. */
    MEAN( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
          new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN ),
          MetricGroup.UNIVARIATE_STATISTIC, MetricGroup.LRB ),

    /** Median statistic. Not currently used to "score" a single side of paired data. */
    MEDIAN( new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN ),
            MetricGroup.UNIVARIATE_STATISTIC ),

    /** Standard deviation statistic. */
    STANDARD_DEVIATION( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
                        new Limits( 0, Double.POSITIVE_INFINITY, Double.NaN ),
                        MetricGroup.UNIVARIATE_STATISTIC, MetricGroup.LRB ),

    /** Minimum statistic. */
    MINIMUM( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
             new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN ),
             MetricGroup.UNIVARIATE_STATISTIC, MetricGroup.LRB ),

    /** Maximum statistic. */
    MAXIMUM( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE,
             new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN ),
             MetricGroup.UNIVARIATE_STATISTIC, MetricGroup.LRB ),

    /** Mean absolute statistic. Not currently used to measure a single side of paired data. */
    MEAN_ABSOLUTE( new Limits( 0, Double.POSITIVE_INFINITY, Double.NaN ), MetricGroup.UNIVARIATE_STATISTIC ),

    /** Histogram. Not currently used to measure a single side of paired data. */
    HISTOGRAM( null, StatisticType.DIAGRAM, MetricGroup.UNIVARIATE_STATISTIC ),

    /** Box plot. Not currently used to measure a single side of paired data. */
    BOX_PLOT( null, StatisticType.BOXPLOT_PER_POOL, MetricGroup.UNIVARIATE_STATISTIC ),

    /** Quantile. Not currently used to measure a single side of paired data. */
    QUANTILE( new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, Double.NaN ),
              MetricGroup.UNIVARIATE_STATISTIC ),

    /** Time to peak error, mean across all instances. */
    TIME_TO_PEAK_ERROR_MEAN( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                             StatisticType.DURATION_SCORE,
                             new Heritage( MetricConstants.TIME_TO_PEAK_ERROR,
                                           MetricConstants.MEAN,
                                           MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ),
                             new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak error, median across all instances. */
    TIME_TO_PEAK_ERROR_MEDIAN( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                               StatisticType.DURATION_SCORE,
                               new Heritage( MetricConstants.TIME_TO_PEAK_ERROR,
                                             MetricConstants.MEDIAN,
                                             MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ),
                               new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak error, minimum across all instances. */
    TIME_TO_PEAK_ERROR_MINIMUM( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                StatisticType.DURATION_SCORE,
                                new Heritage( MetricConstants.TIME_TO_PEAK_ERROR,
                                              MetricConstants.MINIMUM,
                                              MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ),
                                new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak error, maximum across all instances. */
    TIME_TO_PEAK_ERROR_MAXIMUM( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                StatisticType.DURATION_SCORE,
                                new Heritage( MetricConstants.TIME_TO_PEAK_ERROR,
                                              MetricConstants.MAXIMUM,
                                              MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ),
                                new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak error, standard deviation across all instances. */
    TIME_TO_PEAK_ERROR_STANDARD_DEVIATION( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                           StatisticType.DURATION_SCORE,
                                           new Heritage( MetricConstants.TIME_TO_PEAK_ERROR,
                                                         MetricConstants.STANDARD_DEVIATION,
                                                         MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ),
                                           new Limits( 0, Double.POSITIVE_INFINITY, Double.NaN ) ),

    /** Time to peak error, mean absolute value across all instances. */
    TIME_TO_PEAK_ERROR_MEAN_ABSOLUTE( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                      StatisticType.DURATION_SCORE,
                                      new Heritage( MetricConstants.TIME_TO_PEAK_ERROR,
                                                    MetricConstants.MEAN_ABSOLUTE,
                                                    MetricConstants.TIME_TO_PEAK_ERROR_STATISTIC ),
                                      new Limits( 0, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak relative error, mean across all instances. */
    TIME_TO_PEAK_RELATIVE_ERROR_MEAN( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                      StatisticType.DURATION_SCORE,
                                      new Heritage( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR,
                                                    MetricConstants.MEAN,
                                                    MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC ),
                                      new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak relative error, median across all instances. */
    TIME_TO_PEAK_RELATIVE_ERROR_MEDIAN( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                        StatisticType.DURATION_SCORE,
                                        new Heritage( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR,
                                                      MetricConstants.MEDIAN,
                                                      MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC ),
                                        new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak relative error, minimum across all instances. */
    TIME_TO_PEAK_RELATIVE_ERROR_MINIMUM( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                         StatisticType.DURATION_SCORE,
                                         new Heritage( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR,
                                                       MetricConstants.MINIMUM,
                                                       MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC ),
                                         new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak relative error, maximum across all instances. */
    TIME_TO_PEAK_RELATIVE_ERROR_MAXIMUM( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                         StatisticType.DURATION_SCORE,
                                         new Heritage( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR,
                                                       MetricConstants.MAXIMUM,
                                                       MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC ),
                                         new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Time to peak relative error, standard deviation across all instances. */
    TIME_TO_PEAK_RELATIVE_ERROR_STANDARD_DEVIATION( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                                    StatisticType.DURATION_SCORE,
                                                    new Heritage( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR,
                                                                  MetricConstants.STANDARD_DEVIATION,
                                                                  MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC ),
                                                    new Limits( 0, Double.POSITIVE_INFINITY, Double.NaN ) ),

    /** Time to peak relative error, mean absolute value across all instances. */
    TIME_TO_PEAK_RELATIVE_ERROR_MEAN_ABSOLUTE( SampleDataGroup.SINGLE_VALUED_TIME_SERIES,
                                               StatisticType.DURATION_SCORE,
                                               new Heritage( MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR,
                                                             MetricConstants.MEAN_ABSOLUTE,
                                                             MetricConstants.TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC ),
                                               new Limits( Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, 0 ) ),

    /** Indicator for no decomposition. */
    NONE( MetricGroup.NONE ),

    /** Identifier for a Calibration-Refinement (CR) factorization. */
    CR( MetricGroup.CR ),

    /** A Calibration-Refinement (CR) factorization, together with an additional potential score component. */
    CR_POT( MetricGroup.CR_POT ),

    /** A Likelihood-Base-Rate (LBR) factorization. */
    LBR( MetricGroup.LBR ),

    /** The score and components of both the CR and LBR factorizations. */
    CR_AND_LBR( MetricGroup.CR_AND_LBR ),

    /** The main component of a metric, such as the overall score in a score decomposition. */
    MAIN( MetricGroup.NONE, MetricGroup.CR, MetricGroup.CR_POT, MetricGroup.LBR, MetricGroup.CR_AND_LBR ),

    /** The reliability component of a score decomposition. */
    RELIABILITY( MetricGroup.CR, MetricGroup.CR_POT, MetricGroup.CR_AND_LBR ),

    /** The resolution component of a score decomposition. */
    RESOLUTION( MetricGroup.CR, MetricGroup.CR_POT, MetricGroup.CR_AND_LBR ),

    /** The uncertainty component of a score decomposition. */
    UNCERTAINTY( MetricGroup.CR, MetricGroup.CR_POT, MetricGroup.CR_AND_LBR ),

    /** The potential score value (perfect reliability). */
    POTENTIAL( MetricGroup.CR_POT ),

    /** The Type-II conditional bias component of a score decomposition. */
    TYPE_II_CONDITIONAL_BIAS( MetricGroup.LBR, MetricGroup.CR_AND_LBR ),

    /** The discrimination component of a score decomposition. */
    DISCRIMINATION( MetricGroup.LBR, MetricGroup.CR_AND_LBR ),

    /** The sharpness component of a score decomposition. */
    SHARPNESS( MetricGroup.LBR, MetricGroup.CR_AND_LBR ),

    /** True positives. */
    TRUE_POSITIVES( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, MetricGroup.CONTINGENCY_TABLE ),

    /** False positives.*/
    FALSE_POSITIVES( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, MetricGroup.CONTINGENCY_TABLE ),

    /** False negatives.*/
    FALSE_NEGATIVES( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, MetricGroup.CONTINGENCY_TABLE ),

    /** True negatives. */
    TRUE_NEGATIVES( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, MetricGroup.CONTINGENCY_TABLE ),

    /** The component of a univariate statistic that applies to the left-sided data within a pairing. */
    LEFT( MetricGroup.LRB ),

    /** The component of a univariate statistic that applies to the right-sided data within a pairing. */
    RIGHT( MetricGroup.LRB ),

    /** The component of a univariate statistic that applies to the baseline-sided data within a pairing. */
    BASELINE( MetricGroup.LRB );

    /** The {@link SampleDataGroup}. */
    private final SampleDataGroup[] inGroups;

    /** The {@link StatisticType} or null if the {@link MetricConstants} does not belong to a group. */
    private final StatisticType outGroup;

    /** The array of {@link MetricGroup} to which this {@link MetricConstants} belongs. */
    private final MetricGroup[] metricGroups;

    /** Is <code>true</code> if the metric measures skill relative to a baseline, otherwise <code>false</code>. */
    private final boolean isSkillMetric;

    /** A parent metric, may be null. */
    private final MetricConstants parent;

    /** A child metric, may be null. */
    private final MetricConstants child;

    /** A collection metric, may be null. This is used to distinguish a group of statistics that are associated with a
     * common parent metric, but are distinct from the parent metric. For example, a {@link #TIME_TO_PEAK_ERROR_MEAN}
     * is a summary error statistics associated with a {@link #TIME_TO_PEAK_ERROR}, but the {@link #TIME_TO_PEAK_ERROR}
     * is a statistic itself (not summarized). In this case, the {@link #TIME_TO_PEAK_ERROR_STATISTIC} is used to
     * collect the {@link #TIME_TO_PEAK_ERROR_MEAN}, together with all related summary statistics, allowing them to be
     * identified collectively, but separately from the statistics produced by the {@link #TIME_TO_PEAK_ERROR}. */

    private final MetricConstants collection;

    /** The optimum value of the metric. */
    private final double optimum;

    /** The minimum value of the metric. */
    private final double minimum;

    /** The minimum value of the metric. */
    private final double maximum;

    /**
     * Creates an instance.
     *
     * @param inGroup the input group
     * @param outGroup the output group
     */

    MetricConstants( SampleDataGroup inGroup,
                     StatisticType outGroup )
    {
        this( new SampleDataGroup[] { inGroup },
              outGroup,
              false,
              new Heritage( null, null, null ),
              new Limits( Double.NaN, Double.NaN, Double.NaN ) );
    }

    /**
     * Creates an instance.
     *
     * @param inGroup the input group
     * @param outGroup the output group
     * @param isSkillMetric is true if the metric is a skill metric
     * @param limits the limits of the metric
     */

    MetricConstants( SampleDataGroup inGroup,
                     StatisticType outGroup,
                     boolean isSkillMetric,
                     Limits limits )
    {
        this( new SampleDataGroup[] { inGroup },
              outGroup,
              isSkillMetric,
              new Heritage( null, null, null ),
              limits );
    }

    /**
     * Creates an instance.
     *
     * @param inGroup the input group
     * @param outGroup the output group
     * @param metricGroup the metric group
     */

    MetricConstants( SampleDataGroup inGroup,
                     StatisticType outGroup,
                     MetricGroup... metricGroup )
    {
        this( new SampleDataGroup[] { inGroup },
              outGroup,
              false,
              new Heritage( null, null, null ),
              new Limits( Double.NaN, Double.NaN, Double.NaN ),
              metricGroup );
    }

    /**
     * Creates an instance.
     *
     * @param inGroup the input group
     * @param outGroup the output group
     * @param limits the metric limits
     * @param metricGroup the metric group
     */

    MetricConstants( SampleDataGroup inGroup,
                     StatisticType outGroup,
                     Limits limits,
                     MetricGroup... metricGroup )
    {
        this( new SampleDataGroup[] { inGroup },
              outGroup,
              false,
              new Heritage( null, null, null ),
              limits,
              metricGroup );
    }

    /**
     * Creates an instance.
     *
     * @param inGroup the input group
     * @param outGroup the output group
     * @param heritage the metric heritage
     * @param limits the metric limits
     * @param metricGroup the metric group
     */

    MetricConstants( SampleDataGroup inGroup,
                     StatisticType outGroup,
                     Heritage heritage,
                     Limits limits,
                     MetricGroup... metricGroup )
    {
        this( new SampleDataGroup[] { inGroup },
              outGroup,
              false,
              heritage,
              limits,
              metricGroup );
    }

    /**
     * Creates an instance.
     *
     * @param inGroups the input groups
     * @param outGroup the output group
     * @param isSkillMetric is true if the metric is a skill metric, otherwise false
     * @param limits the metric limits
     * @param metricGroup the metric group
     */

    MetricConstants( SampleDataGroup[] inGroups,
                     StatisticType outGroup,
                     boolean isSkillMetric,
                     Limits limits,
                     MetricGroup... metricGroup )
    {
        this( inGroups,
              outGroup,
              isSkillMetric,
              new Heritage( null, null, null ),
              limits,
              metricGroup );
    }

    /**
     * Creates an instance.
     *
     * @param inGroups the input groups
     * @param outGroup the output group
     * @param isSkillMetric is true if the metric is a skill metric, otherwise false
     * @param heritage the heritage of the metric
     * @param limits the limits of the metric
     * @param metricGroup the metric group
     */

    MetricConstants( SampleDataGroup[] inGroups,
                     StatisticType outGroup,
                     boolean isSkillMetric,
                     Heritage heritage,
                     Limits limits,
                     MetricGroup... metricGroup )
    {
        this.inGroups = inGroups;
        this.outGroup = outGroup;
        this.metricGroups = metricGroup;
        this.isSkillMetric = isSkillMetric;
        this.parent = heritage.parent();
        this.child = heritage.child();
        this.collection = heritage.collection();
        this.minimum = limits.minimum();
        this.maximum = limits.maximum();
        this.optimum = limits.optimum();
    }

    /**
     * Creates an instance.
     *
     * @param decGroup the decomposition groups to which the {@link MetricConstants} belongs
     */

    MetricConstants( MetricGroup... decGroup )
    {
        this( new Limits( Double.NaN, Double.NaN, Double.NaN ), decGroup );
    }

    /**
     * Creates an instance.
     *
     * @param limits the metric limits
     * @param decGroup the decomposition groups to which the {@link MetricConstants} belongs
     */

    MetricConstants( Limits limits, MetricGroup... decGroup )
    {
        this.metricGroups = decGroup;
        this.inGroups = null;
        this.outGroup = null;
        this.parent = null;
        this.child = null;
        this.collection = null;
        this.isSkillMetric = false;
        this.minimum = limits.minimum();
        this.maximum = limits.maximum();
        this.optimum = limits.optimum();
    }

    /**
     * Returns true if the input {@link SampleDataGroup} contains the current {@link MetricConstants}, false otherwise.
     *
     * @param inGroup the {@link SampleDataGroup}
     * @return true if the input {@link SampleDataGroup} contains the current {@link MetricConstants}, false otherwise
     * @throws NullPointerException if the input is null
     */

    public boolean isInGroup( SampleDataGroup inGroup )
    {
        Objects.requireNonNull( inGroup );

        return Objects.nonNull( this.inGroups ) && Arrays.asList( this.inGroups ).contains( inGroup );
    }

    /**
     * Returns true if the input {@link StatisticType} contains the current {@link MetricConstants}, false
     * otherwise.
     *
     * @param outGroup the {@link StatisticType}
     * @return true if the input {@link StatisticType} contains the current {@link MetricConstants}, false otherwise
     * @throws NullPointerException if the input is null
     */

    public boolean isInGroup( StatisticType outGroup )
    {
        Objects.requireNonNull( outGroup );

        return this.outGroup == outGroup;
    }

    /**
     * Returns true if the input {@link MetricGroup} contains the current {@link MetricConstants}, false otherwise.
     *
     * @param inGroup the {@link MetricGroup}
     * @return true if the input {@link MetricGroup} contains the current {@link MetricConstants}, false otherwise
     * @throws NullPointerException if the input is null
     */

    public boolean isInGroup( MetricGroup inGroup )
    {
        Objects.requireNonNull( inGroup );

        return Arrays.asList( this.metricGroups ).contains( inGroup );
    }

    /**
     * Returns true if the input {@link SampleDataGroup} and {@link StatisticType} both contain the current
     * {@link MetricConstants}, false otherwise.
     *
     * @param inGroup the {@link SampleDataGroup}
     * @param outGroup the {@link StatisticType}
     * @return true if the input {@link SampleDataGroup} and {@link StatisticType} and both contain the current
     *         {@link MetricConstants}, false otherwise
     * @throws NullPointerException if either input is null
     */

    public boolean isInGroup( SampleDataGroup inGroup, StatisticType outGroup )
    {
        return isInGroup( inGroup ) && isInGroup( outGroup );
    }

    /**
     * Returns true if this is a metric that supports filtering or classifying by threshold (i.e., not including the 
     * "all data" threshold, which all metrics support, by definition).
     *
     * @return true if the metric supports thresholds
     */

    public boolean isAThresholdMetric()
    {
        return !( this.getMetricOutputGroup() == StatisticType.BOXPLOT_PER_PAIR
                  || this.getMetricOutputGroup() == StatisticType.BOXPLOT_PER_POOL
                  || this == MetricConstants.QUANTILE_QUANTILE_DIAGRAM
                  || this == MetricConstants.ENSEMBLE_QUANTILE_QUANTILE_DIAGRAM );
    }

    /**
     * Returns true if this is a continuous metric, false otherwise.
     *
     * @return whether the metric is continuous
     */

    public boolean isContinuous()
    {
        return !this.isInGroup( SampleDataGroup.DICHOTOMOUS )
               && !this.isInGroup( SampleDataGroup.MULTICATEGORY )
               && !this.isInGroup( SampleDataGroup.DISCRETE_PROBABILITY );
    }

    /**
     * Returns whether the metric supports sample uncertainty estimation.
     *
     * @return whether the sampling uncertainties can be estimated
     */

    public boolean isSamplingUncertaintyAllowed()
    {
        return !this.isInGroup( StatisticType.BOXPLOT_PER_PAIR )
               && !this.isInGroup( StatisticType.BOXPLOT_PER_POOL );
    }

    /**
     * Returns the {@link StatisticType} associated with the {@link MetricConstants}.
     *
     * @return the {@link StatisticType}.
     */

    public StatisticType getMetricOutputGroup()
    {
        return this.outGroup;
    }

    /**
     * @return a parent metric or null if none is defined.
     */

    public MetricConstants getParent()
    {
        return this.parent;
    }

    /**
     * @return a child metric or null if none is defined.
     */

    public MetricConstants getChild()
    {
        return this.child;
    }

    /**
     * @return a collection metric or null if none is defined. See {@link #collection} for an explanation.
     */

    public MetricConstants getCollection()
    {
        return this.collection;
    }

    /**
     * @return all the children of this metric.
     */

    public Set<MetricConstants> getChildren()
    {
        return Arrays.stream( MetricConstants.values() )
                     .filter( next -> next.getParent() == this )
                     .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Returns all metric components for all {@link MetricGroup} with which this constant is associated or
     * an empty set if none is defined.
     *
     * @return the components in the {@link MetricGroup}
     */

    public Set<MetricConstants> getAllComponents()
    {
        if ( this.metricGroups.length == 0 )
        {
            return Collections.emptySet();
        }

        return Arrays.stream( this.metricGroups )
                     .flatMap( next -> next.getAllComponents().stream() )
                     .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link SampleDataGroup} and
     * {@link StatisticType}.
     *
     * @param inGroup the {@link SampleDataGroup}
     * @param outGroup the {@link StatisticType}
     * @return the {@link MetricConstants} associated with the current {@link SampleDataGroup}
     * @throws NullPointerException if either input is null
     */

    public static Set<MetricConstants> getMetrics( SampleDataGroup inGroup, StatisticType outGroup )
    {
        Objects.requireNonNull( inGroup );
        Objects.requireNonNull( outGroup );

        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.inGroups ) || !Arrays.asList( a.inGroups ).contains( inGroup )
                           || a.outGroup != outGroup );
        return Collections.unmodifiableSet( all );
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link SampleDataGroup}.
     *
     * @param inGroup the {@link SampleDataGroup}
     * @return the {@link MetricConstants} associated with the current {@link SampleDataGroup}
     * @throws NullPointerException if the input is null
     */

    public static Set<MetricConstants> getMetrics( SampleDataGroup inGroup )
    {
        Objects.requireNonNull( inGroup );

        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.inGroups ) || !Arrays.asList( a.inGroups ).contains( inGroup ) );
        return Collections.unmodifiableSet( all );
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link StatisticType}.
     *
     * @param outGroup the {@link StatisticType}
     * @return the {@link MetricConstants} associated with the current {@link StatisticType}
     * @throws NullPointerException if the input is null
     */

    public static Set<MetricConstants> getMetrics( StatisticType outGroup )
    {
        Objects.requireNonNull( outGroup );

        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.outGroup ) || a.outGroup != outGroup );
        return Collections.unmodifiableSet( all );
    }

    /**
     * Returns <code>true</code> if the metric is a skill metric, otherwise <code>false</code>.
     *
     * @return true if the metric is a skill metric, otherwise false
     */

    public boolean isSkillMetric()
    {
        return this.isSkillMetric;
    }

    /**
     * Returns a canonical metric name.
     *
     * @return a canonical name
     */

    public MetricName getCanonicalName()
    {
        return MetricName.valueOf( this.name() );
    }

    /**
     * Returns the minimum value of the metric or {@link Double#NaN} where undefined.
     * @return the minimum
     */
    public double getMinimum()
    {
        return this.minimum;
    }

    /**
     * Returns the maximum value of the metric or {@link Double#NaN} where undefined.
     * @return the maximum
     */
    public double getMaximum()
    {
        return this.maximum;
    }

    /**
     * Returns the optimum value of the metric or {@link Double#NaN} where undefined.
     * @return the optimum
     */
    public double getOptimum()
    {
        return this.optimum;
    }

    /**
     * Returns a string representation.
     *
     * @return a string representation
     */

    @Override
    public String toString()
    {
        return this.name()
                   .replace( "_", " " );
    }

    /**
     * Type of sample data consumed by a metric.
     */

    public enum SampleDataGroup
    {

        /**
         * Metrics that consume single-valued data.
         */

        SINGLE_VALUED,

        /**
         * Metrics that consume single-valued time-series data.
         */

        SINGLE_VALUED_TIME_SERIES,

        /**
         * Metrics that consume discrete probability data.
         */

        DISCRETE_PROBABILITY,

        /**
         * Metrics that consume dichotomous data.
         */

        DICHOTOMOUS,

        /**
         * Metrics that consume multi-category data.
         */

        MULTICATEGORY,

        /**
         * Metrics that consume ensemble data.
         */

        ENSEMBLE;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link SampleDataGroup}.
         *
         * @return the {@link MetricConstants} associated with the current {@link SampleDataGroup}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
            all.removeIf( a -> Objects.isNull( a.inGroups ) || !Arrays.asList( a.inGroups ).contains( this ) );
            return all;
        }

        /**
         * Returns true if this {@link SampleDataGroup} contains the input {@link MetricConstants}, false otherwise.
         *
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link SampleDataGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getMetrics().contains( input );
        }

        /**
         * Returns a set representation of the enumeration. Contains all elements in {@link SampleDataGroup#values()}.
         *
         * @return a set representation of the elements in this enumeration
         */

        public static Set<SampleDataGroup> set()
        {
            return Set.of( SampleDataGroup.values() );
        }

    }

    /**
     * Type of statistic.
     */

    public enum StatisticType
    {

        /**
         * Metrics that produce a {@link DiagramStatistic}.
         */

        DIAGRAM,

        /**
         * Metrics that produce a {@link BoxplotStatistic} for each pair within a pool.
         */

        BOXPLOT_PER_PAIR,

        /**
         * Metrics that produce a {@link BoxplotStatistic} for each pool of pairs.
         */

        BOXPLOT_PER_POOL,

        /**
         * Metrics that produce a {@link DurationDiagramStatistic}.
         */

        DURATION_DIAGRAM,

        /**
         * Metrics that produce a {@link DoubleScoreStatistic}.
         */

        DOUBLE_SCORE,

        /**
         * Metrics that produce a {@link DurationScoreStatistic}.
         */

        DURATION_SCORE;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link StatisticType}.
         *
         * @return the {@link MetricConstants} associated with the current {@link StatisticType}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
            all.removeIf( a -> a.outGroup != this );
            return all;
        }

        /**
         * Returns true if this {@link StatisticType} contains the input {@link MetricConstants}, false otherwise.
         *
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link StatisticType} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getMetrics().contains( input );
        }

        /**
         * Returns a set representation of the enumeration. Contains all elements in {@link StatisticType#values()}.
         *
         * @return a set representation of the elements in this enumeration
         */

        public static Set<StatisticType> set()
        {
            return Set.of( StatisticType.values() );
        }

    }

    /**
     * Collects together multiple parts of a single metric.
     */

    public enum MetricGroup
    {

        /**
         * Indicator for no decomposition.
         */

        NONE,

        /**
         * Identifier for a Calibration-Refinement (CR) factorization into reliability - resolution + uncertainty,
         * comprising the overall score followed by the three components in that order.
         */

        CR,

        /**
         * Identifier for a Calibration-Refinement (CR) factorization into reliability - resolution + uncertainty,
         * comprising the overall score followed by the three components in that order, together with an additional
         * potential score component, where potential = actual score - reliability.
         */

        CR_POT,

        /**
         * Identifier for a Likelihood-Base-Rate (LBR) factorization into Type-II conditional bias - discrimination +
         * sharpness, comprising the overall score followed by the three components in that order.
         */

        LBR,

        /**
         * Identifier for the score and components of both the CR and LBR factorizations.
         */

        CR_AND_LBR,

        /**
         * Identifier for a univariate statistic, such as the mean or median.
         */

        UNIVARIATE_STATISTIC,

        /**
         * Identifier for a Left/right/baseline decomposition.
         */

        LRB,

        /**
         * Identifier for a contingency table group.
         */

        CONTINGENCY_TABLE;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link MetricGroup}.
         *
         * @return the {@link MetricConstants} associated with the current {@link MetricGroup}
         */

        public Set<MetricConstants> getAllComponents()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );

            //Remove metrics that don't match
            all.removeIf( a -> Objects.isNull( a.metricGroups ) || a.name().equals( name() )
                               || !Arrays.asList( a.metricGroups ).contains( this ) );

            return all;
        }

        /**
         * Returns true if this {@link MetricGroup} contains the input {@link MetricConstants}, false
         * otherwise.
         *
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricGroup} contains the input {@link MetricConstants}, false
         *         otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getAllComponents().contains( input );
        }

    }

    /**
     * A dimension associated with a verification metric. The natural order of an enum type is dictated by the order 
     * of declaration. The general rule for diagram outputs is to specify the domain axis first and the range axis 
     * second, i.e. the declaration order should reflect the desired order of the outputs for storage in a map, for 
     * example.
     */

    public enum MetricDimension
    {
        /** Probability of false detection. */
        PROBABILITY_OF_FALSE_DETECTION,

        /** Probability of detection. */
        PROBABILITY_OF_DETECTION,

        /** Rank order. */
        RANK_ORDER,

        /** Forecast probability. */
        FORECAST_PROBABILITY,

        /** Observed relative frequency with which an event occurs. */
        OBSERVED_RELATIVE_FREQUENCY,

        /** Observed quantiles. */
        OBSERVED_QUANTILES,

        /** Predicted quantiles. */
        PREDICTED_QUANTILES,

        /** Forecast error. */
        FORECAST_ERROR,

        /** Observed value. */
        OBSERVED_VALUE,

        /** Forecast value. */
        FORECAST_VALUE,

        /** Ensemble mean. */
        ENSEMBLE_MEAN,

        /** Ensemble median. */
        ENSEMBLE_MEDIAN,

        /** Sample size. */
        SAMPLE_SIZE,

        /** True positives. */
        TRUE_POSITIVES,

        /** False positives. */
        FALSE_POSITIVES,

        /** False negatives. */
        FALSE_NEGATIVES,

        /** True negatives. */
        TRUE_NEGATIVES,

        /** Error as a percentage of the verifying value. */
        ERROR_PERCENT_OF_VERIFYING_VALUE,

        /** The upper bound of a histogram bin. */
        BIN_UPPER_BOUND,

        /** A count. */
        COUNT,

        /** A statistic. */
        STATISTIC;

        /**
         * Returns a string representation.
         *
         * @return a string representation
         */

        @Override
        public String toString()
        {
            return name().replace( "_", " " );
        }
    }

    /**
     * The limits of the metric.
     * @param minimum the minimum value
     * @param maximum the maximum value
     * @param optimum the optimum value
     */
    private record Limits( double minimum, double maximum, double optimum )
    {
    }

    /**
     * The heritage of the metric.
     * @param parent the parent metric, may be null
     * @param child the child metric, may be null
     * @param collection the name used to collect the statistics associated with the parent, but are distinct from it
     */
    private record Heritage( MetricConstants parent,
                             MetricConstants child,
                             MetricConstants collection )
    {
    }
}
