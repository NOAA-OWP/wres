package wres.datamodel.metrics;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DiagramStatistic;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.statistics.generated.BoxplotStatistic;

/**
 * Metric constants. The metric identifiers are grouped by metric input/output type, as defined by the
 * {@link SampleDataGroup} and {@link StatisticType}, respectively.
 * 
 * @author james.brown@hydrosolved.com
 */

public enum MetricConstants
{

    /**
     * Identifier for fractional bias or relative mean error.
     */

    BIAS_FRACTION( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Brier Score.
     */

    BRIER_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Brier Skill Score.
     */

    BRIER_SKILL_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE, true ),

    /**
     * Identifier for a box plot of errors by observed value.
     */

    BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE( SampleDataGroup.ENSEMBLE, StatisticType.BOXPLOT_PER_PAIR ),

    /**
     * Identifier for a box plot of errors by forecast value.
     */

    BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE( SampleDataGroup.ENSEMBLE, StatisticType.BOXPLOT_PER_PAIR ),

    /**
     * Identifier for a box plot of errors.
     */

    BOX_PLOT_OF_ERRORS( SampleDataGroup.SINGLE_VALUED, StatisticType.BOXPLOT_PER_POOL ),

    /**
     * Identifier for a box plot of errors as a percentage of the left value.
     */

    BOX_PLOT_OF_PERCENTAGE_ERRORS( SampleDataGroup.SINGLE_VALUED, StatisticType.BOXPLOT_PER_POOL ),

    /**
     * Identifier for coefficient of determination.
     */

    COEFFICIENT_OF_DETERMINATION( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Continuous Ranked Probability Score
     */

    CONTINUOUS_RANKED_PROBABILITY_SCORE( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Continuous Ranked Probability Skill Score
     */

    CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE( SampleDataGroup.ENSEMBLE, StatisticType.DOUBLE_SCORE, true ),

    /**
     * Identifier for a Contingency Table.
     */

    CONTINGENCY_TABLE( new SampleDataGroup[] { SampleDataGroup.DICHOTOMOUS,
                                               SampleDataGroup.MULTICATEGORY },
            StatisticType.DOUBLE_SCORE, false, MetricGroup.CONTINGENCY_TABLE ),

    /**
     * Identifier for Pearson's product-moment correlation coefficient.
     */

    PEARSON_CORRELATION_COEFFICIENT( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Threat Score.
     */

    THREAT_SCORE( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for an Equitable Threat Score.
     */

    EQUITABLE_THREAT_SCORE( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, true ),

    /**
     * Identifier for a Frequency Bias.
     */

    FREQUENCY_BIAS( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for an Index of Agreement.
     */

    INDEX_OF_AGREEMENT( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for the Kling-Gupta Efficiency index.
     */

    KLING_GUPTA_EFFICIENCY( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, true ),

    /**
     * Identifier for a Mean Absolute Error.
     */

    MEAN_ABSOLUTE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Error.
     */

    MEAN_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Square Error.
     */

    MEAN_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Square Error Skill Score.
     */

    MEAN_SQUARE_ERROR_SKILL_SCORE( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, true ),

    /**
     * Identifier for a Mean Square Error Skill Score, normalized.
     */

    MEAN_SQUARE_ERROR_SKILL_SCORE_NORMALIZED( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, true ),

    /**
     * Identifier for a Median Error.
     */

    MEDIAN_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Peirce Skill Score.
     */

    PEIRCE_SKILL_SCORE( new SampleDataGroup[] { SampleDataGroup.DICHOTOMOUS, SampleDataGroup.MULTICATEGORY },
            StatisticType.DOUBLE_SCORE, true ),

    /**
     * Identifier for a Probability Of Detection.
     */

    PROBABILITY_OF_DETECTION( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Probability Of False Detection.
     */

    PROBABILITY_OF_FALSE_DETECTION( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE ),

    /**
     * Quantile-quantile diagram.
     */

    QUANTILE_QUANTILE_DIAGRAM( SampleDataGroup.SINGLE_VALUED, StatisticType.DIAGRAM ),

    /**
     * Identifier for the Rank Histogram.
     */

    RANK_HISTOGRAM( SampleDataGroup.ENSEMBLE, StatisticType.DIAGRAM ),

    /**
     * Identifier for the Relative Operating Characteristic.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DIAGRAM ),

    /**
     * Identifier for the Relative Operating Characteristic Score.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DOUBLE_SCORE, true ),

    /**
     * Identifier for the Reliability Diagram.
     */

    RELIABILITY_DIAGRAM( SampleDataGroup.DISCRETE_PROBABILITY, StatisticType.DIAGRAM ),

    /**
     * Identifier for a Root Mean Square Error.
     */

    ROOT_MEAN_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for a Root Mean Square Error normalized by the standard deviation of
     * the left values.
     */

    ROOT_MEAN_SQUARE_ERROR_NORMALIZED( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for the sample size.
     */

    SAMPLE_SIZE( new SampleDataGroup[] { SampleDataGroup.SINGLE_VALUED,
                                         SampleDataGroup.ENSEMBLE },
            StatisticType.DOUBLE_SCORE, false, MetricGroup.UNIVARIATE_STATISTIC ),

    /**
     * Identifier for a Sum of Square Error.
     */

    SUM_OF_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for the Volumetric Efficiency.
     */

    VOLUMETRIC_EFFICIENCY( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE ),

    /**
     * Identifier for the Time-to-Peak Error.
     */

    TIME_TO_PEAK_ERROR( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_DIAGRAM ),

    /**
     * Identifier for a statistic derived from the Time-to-Peak Error.
     */

    TIME_TO_PEAK_ERROR_STATISTIC( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_SCORE ),

    /**
     * Identifier for the Time-to-Peak Relative Error.
     */

    TIME_TO_PEAK_RELATIVE_ERROR( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_DIAGRAM ),

    /**
     * Identifier for a statistic derived from the Time-to-Peak Relative Error.
     */

    TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticType.DURATION_SCORE ),

    /**
     * Mean statistic.
     */

    MEAN( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, MetricGroup.UNIVARIATE_STATISTIC,
            MetricGroup.LRB ),

    /**
     * Median statistic.
     */

    MEDIAN( MetricGroup.UNIVARIATE_STATISTIC ),

    /**
     * Standard deviation statistic.
     */

    STANDARD_DEVIATION( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, MetricGroup.UNIVARIATE_STATISTIC,
            MetricGroup.LRB ),

    /**
     * Minimum statistic.
     */

    MINIMUM( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, MetricGroup.UNIVARIATE_STATISTIC,
            MetricGroup.LRB ),

    /**
     * Maximum statistic.
     */

    MAXIMUM( SampleDataGroup.SINGLE_VALUED, StatisticType.DOUBLE_SCORE, MetricGroup.UNIVARIATE_STATISTIC,
            MetricGroup.LRB ),

    /**
     * Mean absolute statistic.
     */

    MEAN_ABSOLUTE( MetricGroup.UNIVARIATE_STATISTIC ),

    /**
     * Indicator for no decomposition.
     */

    NONE( MetricGroup.NONE ),

    /**
     * Identifier for a Calibration-Refinement (CR) factorization.
     */

    CR( MetricGroup.CR ),

    /**
     * Identifier for a Calibration-Refinement (CR) factorization, together with an additional potential score
     * component.
     */

    CR_POT( MetricGroup.CR_POT ),

    /**
     * Identifier for a Likelihood-Base-Rate (LBR) factorization.
     */

    LBR( MetricGroup.LBR ),

    /**
     * Identifier for the score and components of both the CR and LBR factorizations.
     */

    CR_AND_LBR( MetricGroup.CR_AND_LBR ),

    /**
     * Identifier for the main component of a metric, such as the overall score in a score decomposition.
     */

    MAIN( MetricGroup.NONE, MetricGroup.CR, MetricGroup.CR_POT, MetricGroup.LBR, MetricGroup.CR_AND_LBR ),

    /**
     * Identifier for the reliability component of a score decomposition.
     */

    RELIABILITY( MetricGroup.CR, MetricGroup.CR_POT, MetricGroup.CR_AND_LBR ),

    /**
     * Identifier for the resolution component of a score decomposition.
     */

    RESOLUTION( MetricGroup.CR, MetricGroup.CR_POT, MetricGroup.CR_AND_LBR ),

    /**
     * Identifier for the uncertainty component of a score decomposition.
     */

    UNCERTAINTY( MetricGroup.CR, MetricGroup.CR_POT, MetricGroup.CR_AND_LBR ),

    /**
     * Identifier for the potential score value (perfect reliability).
     */

    POTENTIAL( MetricGroup.CR_POT ),

    /**
     * Identifier for the Type-II conditional bias component of a score decomposition.
     */

    TYPE_II_CONDITIONAL_BIAS( MetricGroup.LBR, MetricGroup.CR_AND_LBR ),

    /**
     * Identifier for the discrimination component of a score decomposition.
     */

    DISCRIMINATION( MetricGroup.LBR, MetricGroup.CR_AND_LBR ),

    /**
     * Identifier for the sharpness component of a score decomposition.
     */

    SHARPNESS( MetricGroup.LBR, MetricGroup.CR_AND_LBR ),

    /**
     * Identifier for true positives.
     */

    TRUE_POSITIVES( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, MetricGroup.CONTINGENCY_TABLE ),

    /**
     * Identifier for false positives.
     */

    FALSE_POSITIVES( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, MetricGroup.CONTINGENCY_TABLE ),

    /**
     * Identifier for false negatives.
     */

    FALSE_NEGATIVES( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, MetricGroup.CONTINGENCY_TABLE ),

    /**
     * Identifier for true negatives.
     */

    TRUE_NEGATIVES( SampleDataGroup.DICHOTOMOUS, StatisticType.DOUBLE_SCORE, MetricGroup.CONTINGENCY_TABLE ),

    /**
     * Identifier for the component of a univariate statistic that applies to the left-sided data within a pairing.
     */

    LEFT( MetricGroup.LRB ),

    /**
     * Identifier for the component of a univariate statistic that applies to the right-sided data within a pairing.
     */

    RIGHT( MetricGroup.LRB ),

    /**
     * Identifier for the component of a univariate statistic that applies to the baseline-sided data within a pairing.
     */

    BASELINE( MetricGroup.LRB );

    /**
     * The {@link SampleDataGroup}.
     */

    private final SampleDataGroup[] inGroups;

    /**
     * The {@link StatisticType} or null if the {@link MetricConstants} does not belong to a group.
     */

    private final StatisticType outGroup;

    /**
     * The array of {@link MetricGroup} to which this {@link MetricConstants} belongs.
     */

    private final MetricGroup[] metricGroups;

    /**
     * Is <code>true</code> if the metric measures skill relative to a baseline, otherwise <code>false</code>.
     */

    private final boolean isSkillMetric;

    /**
     * Default constructor
     */

    private MetricConstants()
    {
        this.inGroups = new SampleDataGroup[0];
        this.outGroup = null;
        this.metricGroups = new MetricGroup[0];
        this.isSkillMetric = false;
    }

    /**
     * Construct with a {@link SampleDataGroup} and a {@link StatisticType} and whether the metric measures skill.
     * 
     * @param inputGroup the input group
     * @param outputGroup the output group
     * @param isSkillMetric is true if the metric is a skill metric
     */

    private MetricConstants( SampleDataGroup inGroup, StatisticType outGroup, boolean isSkillMetric )
    {
        this( new SampleDataGroup[] { inGroup }, outGroup, isSkillMetric, new MetricGroup[0] );
    }

    /**
     * Construct with a {@link SampleDataGroup} and a {@link StatisticType}.
     * 
     * @param inputGroup the input group
     * @param outputGroup the output group
     * @param metricGroup the metric group
     */

    private MetricConstants( SampleDataGroup inGroup, StatisticType outGroup, MetricGroup... metricGroup )
    {
        this( new SampleDataGroup[] { inGroup }, outGroup, false, metricGroup );
    }

    /**
     * Construct with multiple {@link SampleDataGroup} and a {@link StatisticType}.
     * 
     * @param inGroups the input groups
     * @param outGroup the output group
     * @param isSkillMetric is true if the metric measures skill, otherwise false
     * @param metricGroup the metric group
     */

    private MetricConstants( SampleDataGroup[] inGroups,
                             StatisticType outGroup,
                             boolean isSkillMetric,
                             MetricGroup... metricGroup )
    {
        this.inGroups = inGroups;
        this.outGroup = outGroup;
        this.metricGroups = metricGroup;
        this.isSkillMetric = isSkillMetric;
    }

    /**
     * Construct with a varargs of {@link MetricGroup}.
     * 
     * @param decGroup the decomposition groups to which the {@link MetricConstants} belongs
     */

    private MetricConstants( MetricGroup... decGroup )
    {
        this.metricGroups = decGroup;
        this.inGroups = null;
        this.outGroup = null;
        this.isSkillMetric = false;
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

        return Arrays.asList( this.inGroups ).contains( inGroup );
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
        return ! ( this.getMetricOutputGroup() == StatisticType.BOXPLOT_PER_PAIR
                   || this.getMetricOutputGroup() == StatisticType.BOXPLOT_PER_POOL
                   || this == MetricConstants.QUANTILE_QUANTILE_DIAGRAM );
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
     * Returns <code>true</code> if the metric measures skill, otherwise <code>false</code>.
     * 
     * @return true if the metric measures skill, otherwise false
     */

    public boolean isSkillMetric()
    {
        return this.isSkillMetric;
    }

    /**
     * Returns a string representation.
     * 
     * @return a string representation
     */

    @Override
    public String toString()
    {
        return name().replaceAll( "_", " " );
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
            return Collections.unmodifiableSet( new HashSet<>( Arrays.asList( SampleDataGroup.values() ) ) );
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
         * Metrics that produce a {@link DurationDiagramStatisticOuter}.
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
            return Collections.unmodifiableSet( new HashSet<>( Arrays.asList( StatisticType.values() ) ) );
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

        /**
         * Identifier for probability of false detection.
         */

        PROBABILITY_OF_FALSE_DETECTION,

        /**
         * Identifier for probability of detection.
         */

        PROBABILITY_OF_DETECTION,

        /**
         * Identifier for a rank ordering.
         */

        RANK_ORDER,

        /**
         * Identifier for a forecast probability.
         */

        FORECAST_PROBABILITY,

        /**
         * Identifier for the observed relative frequency with which an event occurs.
         */

        OBSERVED_RELATIVE_FREQUENCY,

        /**
         * Identifier for observed quantiles.
         */

        OBSERVED_QUANTILES,

        /**
         * Identifier for predicted quantiles.
         */

        PREDICTED_QUANTILES,

        /**
         * Identifier for error.
         */

        FORECAST_ERROR,

        /**
         * Identifier for observed value. 
         */

        OBSERVED_VALUE,

        /**
         * Identifier for forecast value. 
         */

        FORECAST_VALUE,

        /**
         * Identifier for ensemble mean. 
         */

        ENSEMBLE_MEAN,

        /**
         * Identifier for ensemble median. 
         */

        ENSEMBLE_MEDIAN,

        /**
         * Identifier for a sample size.
         */

        SAMPLE_SIZE,

        /**
         * Identifier for true positives.
         */

        TRUE_POSITIVES,

        /**
         * Identifier for false positives.
         */

        FALSE_POSITIVES,

        /**
         * Identifier for false negatives.
         */

        FALSE_NEGATIVES,

        /**
         * Identifier for true negatives.
         */

        TRUE_NEGATIVES,

        /**
         * Identifier for error as a percentage of the verifying value.
         */

        ERROR_PERCENT_OF_VERIFYING_VALUE;

        /**
         * Returns a string representation.
         * 
         * @return a string representation
         */

        @Override
        public String toString()
        {
            return name().replaceAll( "_", " " );
        }

    }

}