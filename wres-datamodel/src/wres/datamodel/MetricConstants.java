package wres.datamodel;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import wres.datamodel.statistics.BoxPlotStatistic;
import wres.datamodel.statistics.DoubleScoreStatistic;
import wres.datamodel.statistics.DurationScoreStatistic;
import wres.datamodel.statistics.MatrixStatistic;
import wres.datamodel.statistics.MultiVectorStatistic;
import wres.datamodel.statistics.PairedStatistic;

/**
 * Metric constants. The metric identifiers are grouped by metric input/output type, as defined by the
 * {@link SampleDataGroup} and {@link StatisticGroup}, respectively.
 * 
 * @author james.brown@hydrosolved.com
 */

public enum MetricConstants
{

    /**
     * Identifier for fractional bias or relative mean error.
     */

    BIAS_FRACTION( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Brier Score.
     */

    BRIER_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Brier Skill Score.
     */

    BRIER_SKILL_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a box plot of errors by observed value.
     */

    BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE( SampleDataGroup.ENSEMBLE, StatisticGroup.BOXPLOT ),

    /**
     * Identifier for a box plot of errors by forecast value.
     */

    BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE( SampleDataGroup.ENSEMBLE, StatisticGroup.BOXPLOT ),

    /**
     * Identifier for coefficient of determination.
     */

    COEFFICIENT_OF_DETERMINATION( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Continuous Ranked Probability Score
     */

    CONTINUOUS_RANKED_PROBABILITY_SCORE( SampleDataGroup.ENSEMBLE, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Continuous Ranked Probability Skill Score
     */

    CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE( SampleDataGroup.ENSEMBLE, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Contingency Table.
     */

    CONTINGENCY_TABLE( new SampleDataGroup[] { SampleDataGroup.DICHOTOMOUS,
                                                SampleDataGroup.MULTICATEGORY },
            StatisticGroup.MATRIX ),

    /**
     * Identifier for Pearson's product-moment correlation coefficient.
     */

    PEARSON_CORRELATION_COEFFICIENT( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Threat Score.
     */

    THREAT_SCORE( SampleDataGroup.DICHOTOMOUS, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for an Equitable Threat Score.
     */

    EQUITABLE_THREAT_SCORE( SampleDataGroup.DICHOTOMOUS, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Frequency Bias.
     */

    FREQUENCY_BIAS( SampleDataGroup.DICHOTOMOUS, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for an Index of Agreement.
     */

    INDEX_OF_AGREEMENT( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the Kling-Gupta Efficiency index.
     */

    KLING_GUPTA_EFFICIENCY( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Absolute Error.
     */

    MEAN_ABSOLUTE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Error.
     */

    MEAN_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Square Error.
     */

    MEAN_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Square Error Skill Score.
     */

    MEAN_SQUARE_ERROR_SKILL_SCORE( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Peirce Skill Score.
     */

    PEIRCE_SKILL_SCORE( new SampleDataGroup[] { SampleDataGroup.DICHOTOMOUS, SampleDataGroup.MULTICATEGORY },
            StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Probability Of Detection.
     */

    PROBABILITY_OF_DETECTION( SampleDataGroup.DICHOTOMOUS, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Probability Of False Detection.
     */

    PROBABILITY_OF_FALSE_DETECTION( SampleDataGroup.DICHOTOMOUS, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Quantile-quantile diagram.
     */

    QUANTILE_QUANTILE_DIAGRAM( SampleDataGroup.SINGLE_VALUED, StatisticGroup.MULTIVECTOR ),

    /**
     * Identifier for the Rank Histogram.
     */

    RANK_HISTOGRAM( SampleDataGroup.ENSEMBLE, StatisticGroup.MULTIVECTOR ),

    /**
     * Identifier for the Relative Operating Characteristic.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM( SampleDataGroup.DISCRETE_PROBABILITY, StatisticGroup.MULTIVECTOR ),

    /**
     * Identifier for the Relative Operating Characteristic Score.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_SCORE( SampleDataGroup.DISCRETE_PROBABILITY, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the Reliability Diagram.
     */

    RELIABILITY_DIAGRAM( SampleDataGroup.DISCRETE_PROBABILITY, StatisticGroup.MULTIVECTOR ),

    /**
     * Identifier for a Root Mean Square Error.
     */

    ROOT_MEAN_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the sample size.
     */

    SAMPLE_SIZE( new SampleDataGroup[] { SampleDataGroup.SINGLE_VALUED,
                                          SampleDataGroup.ENSEMBLE },
            StatisticGroup.DOUBLE_SCORE, ScoreGroup.UNIVARIATE_STATISTIC ),

    /**
     * Identifier for a Sum of Square Error.
     */

    SUM_OF_SQUARE_ERROR( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the Volumetric Efficiency.
     */

    VOLUMETRIC_EFFICIENCY( SampleDataGroup.SINGLE_VALUED, StatisticGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the Time-to-Peak Error.
     */

    TIME_TO_PEAK_ERROR( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticGroup.PAIRED ),

    /**
     * Identifier for a statistic derived from the Time-to-Peak Error.
     */

    TIME_TO_PEAK_ERROR_STATISTIC( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticGroup.DURATION_SCORE ),

    /**
     * Identifier for the Time-to-Peak Relative Error.
     */

    TIME_TO_PEAK_RELATIVE_ERROR( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticGroup.PAIRED ),

    /**
     * Identifier for a statistic derived from the Time-to-Peak Relative Error.
     */

    TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC( SampleDataGroup.SINGLE_VALUED_TIME_SERIES, StatisticGroup.DURATION_SCORE ),

    /**
     * Mean statistic.
     */

    MEAN( ScoreGroup.UNIVARIATE_STATISTIC ),

    /**
     * Median statistic.
     */

    MEDIAN( ScoreGroup.UNIVARIATE_STATISTIC ),

    /**
     * Standard deviation statistic.
     */

    STANDARD_DEVIATION( ScoreGroup.UNIVARIATE_STATISTIC ),

    /**
     * Minimum statistic.
     */

    MINIMUM( ScoreGroup.UNIVARIATE_STATISTIC ),

    /**
     * Maximum statistic.
     */

    MAXIMUM( ScoreGroup.UNIVARIATE_STATISTIC ),

    /**
     * Mean absolute statistic.
     */

    MEAN_ABSOLUTE( ScoreGroup.UNIVARIATE_STATISTIC ),

    /**
     * Indicator for no decomposition.
     */

    NONE( ScoreGroup.NONE ),

    /**
     * Identifier for a Calibration-Refinement (CR) factorization.
     */

    CR( ScoreGroup.CR ),

    /**
     * Identifier for a Calibration-Refinement (CR) factorization, together with an additional potential score
     * component.
     */

    CR_POT( ScoreGroup.CR_POT ),

    /**
     * Identifier for a Likelihood-Base-Rate (LBR) factorization.
     */

    LBR( ScoreGroup.LBR ),

    /**
     * Identifier for the score and components of both the CR and LBR factorizations.
     */

    CR_AND_LBR( ScoreGroup.CR_AND_LBR ),

    /**
     * Identifier for the main component of a metric, such as the overall score in a score decomposition.
     */

    MAIN( ScoreGroup.NONE, ScoreGroup.CR, ScoreGroup.CR_POT, ScoreGroup.LBR, ScoreGroup.CR_AND_LBR ),

    /**
     * Identifier for the reliability component of a score decomposition.
     */

    RELIABILITY( ScoreGroup.CR, ScoreGroup.CR_POT, ScoreGroup.CR_AND_LBR ),

    /**
     * Identifier for the resolution component of a score decomposition.
     */

    RESOLUTION( ScoreGroup.CR, ScoreGroup.CR_POT, ScoreGroup.CR_AND_LBR ),

    /**
     * Identifier for the uncertainty component of a score decomposition.
     */

    UNCERTAINTY( ScoreGroup.CR, ScoreGroup.CR_POT, ScoreGroup.CR_AND_LBR ),

    /**
     * Identifier for the potential score value (perfect reliability).
     */

    POTENTIAL( ScoreGroup.CR_POT ),

    /**
     * Identifier for the Type-II conditional bias component of a score decomposition.
     */

    TYPE_II_CONDITIONAL_BIAS( ScoreGroup.LBR, ScoreGroup.CR_AND_LBR ),

    /**
     * Identifier for the discrimination component of a score decomposition.
     */

    DISCRIMINATION( ScoreGroup.LBR, ScoreGroup.CR_AND_LBR ),

    /**
     * Identifier for the sharpness component of a score decomposition.
     */

    SHARPNESS( ScoreGroup.LBR, ScoreGroup.CR_AND_LBR );

    /**
     * The {@link SampleDataGroup} or null if the {@link MetricConstants} does not belong to a group.
     */

    private final SampleDataGroup[] inGroup;

    /**
     * The {@link StatisticGroup} or null if the {@link MetricConstants} does not belong to a group.
     */

    private final StatisticGroup outGroup;

    /**
     * The {@link ScoreGroup} to which this {@link MetricConstants} belongs or null if the
     * {@link MetricConstants} does not belong to a {@link ScoreGroup}.
     */

    private final ScoreGroup[] scoreTypeGroup;

    /**
     * Default constructor
     */

    private MetricConstants()
    {
        this.inGroup = null;
        this.outGroup = null;
        this.scoreTypeGroup = null;
    }

    /**
     * Construct with a {@link SampleDataGroup} and a {@link StatisticGroup}.
     * 
     * @param inputGroup the input group
     * @param outputGroup the output group
     */

    private MetricConstants( SampleDataGroup inGroup, StatisticGroup outGroup )
    {
        this( new SampleDataGroup[] { inGroup }, outGroup, (ScoreGroup[]) null );
    }

    /**
     * Construct with multiple {@link SampleDataGroup} and a {@link StatisticGroup}.
     * 
     * @param inGroups the input groups
     * @param secondGroup the second input group
     * @param outputGroup the output group
     */

    private MetricConstants( SampleDataGroup[] inGroups,
                             StatisticGroup outGroup,
                             ScoreGroup... scoreTypeGroup )
    {
        this.inGroup = inGroups;
        this.outGroup = outGroup;
        this.scoreTypeGroup = scoreTypeGroup;
    }

    /**
     * Construct with a varargs of {@link ScoreGroup}.
     * 
     * @param decGroup the decomposition groups to which the {@link MetricConstants} belongs
     */

    private MetricConstants( ScoreGroup... decGroup )
    {
        this.scoreTypeGroup = decGroup;
        this.inGroup = null;
        this.outGroup = null;
    }

    /**
     * Returns true if the input {@link SampleDataGroup} contains the current {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link SampleDataGroup}
     * @return true if the input {@link SampleDataGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( SampleDataGroup inGroup )
    {
        return Arrays.asList( this.inGroup ).contains( inGroup );
    }

    /**
     * Returns true if the input {@link StatisticGroup} contains the current {@link MetricConstants}, false
     * otherwise.
     * 
     * @param outGroup the {@link StatisticGroup}
     * @return true if the input {@link StatisticGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( StatisticGroup outGroup )
    {
        return this.outGroup == outGroup;
    }

    /**
     * Returns true if the input {@link ScoreGroup} contains the current {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link ScoreGroup}
     * @return true if the input {@link ScoreGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( ScoreGroup inGroup )
    {
        return Arrays.asList( this.scoreTypeGroup ).contains( inGroup );
    }

    /**
     * Returns true if the input {@link SampleDataGroup} and {@link StatisticGroup} both contain the current
     * {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link SampleDataGroup}
     * @param outGroup the {@link StatisticGroup}
     * @return true if the input {@link SampleDataGroup} and {@link StatisticGroup} and both contain the current
     *         {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( SampleDataGroup inGroup, StatisticGroup outGroup )
    {
        return isInGroup( inGroup ) && isInGroup( outGroup );
    }

    /**
     * Returns the {@link StatisticGroup} associated with the {@link MetricConstants}.
     * 
     * @return the {@link StatisticGroup}.
     */

    public StatisticGroup getMetricOutputGroup()
    {
        return outGroup;
    }

    /**
     * Returns all metric components in the {@link ScoreGroup} with which this constant is associated or
     * null if none is defined.
     * 
     * @return the components in the {@link ScoreGroup} or null
     */

    public Set<MetricConstants> getAllComponents()
    {
        return Objects.isNull( scoreTypeGroup ) ? null : scoreTypeGroup[0].getAllComponents();
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link SampleDataGroup} and
     * {@link StatisticGroup}.
     * 
     * @param inGroup the {@link SampleDataGroup}
     * @param outGroup the {@link StatisticGroup}
     * @return the {@link MetricConstants} associated with the current {@link SampleDataGroup}
     */

    public static Set<MetricConstants> getMetrics( SampleDataGroup inGroup, StatisticGroup outGroup )
    {
        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.inGroup ) || !Arrays.asList( a.inGroup ).contains( inGroup )
                           || a.outGroup != outGroup );
        return Collections.unmodifiableSet( all );
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link SampleDataGroup}.
     * 
     * @param inGroup the {@link SampleDataGroup}
     * @return the {@link MetricConstants} associated with the current {@link SampleDataGroup}
     */

    public static Set<MetricConstants> getMetrics( SampleDataGroup inGroup )
    {
        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.inGroup ) || !Arrays.asList( a.inGroup ).contains( inGroup ) );
        return Collections.unmodifiableSet( all );
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link StatisticGroup}.
     * 
     * @param outGroup the {@link StatisticGroup}
     * @return the {@link MetricConstants} associated with the current {@link StatisticGroup}
     */

    public static Set<MetricConstants> getMetrics( StatisticGroup outGroup )
    {
        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.outGroup ) || a.outGroup != outGroup );
        return Collections.unmodifiableSet( all );
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
            all.removeIf( a -> Objects.isNull( a.inGroup ) || !Arrays.asList( a.inGroup ).contains( this ) );
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

    public enum StatisticGroup
    {

        /**
         * Metrics that produce a {@link MultiVectorStatistic}.
         */

        MULTIVECTOR,

        /**
         * Metrics that produce a {@link MatrixStatistic}.
         */

        MATRIX,

        /**
         * Metrics that produce a {@link BoxPlotStatistic}.
         */

        BOXPLOT,

        /**
         * Metrics that produce a {@link PairedStatistic}.
         */

        PAIRED,

        /**
         * Metrics that produce a {@link DoubleScoreStatistic}.
         */

        DOUBLE_SCORE,

        /**
         * Metrics that produce a {@link DurationScoreStatistic}.
         */

        DURATION_SCORE;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link StatisticGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link StatisticGroup}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
            all.removeIf( a -> a.outGroup != this );
            return all;
        }

        /**
         * Returns true if this {@link StatisticGroup} contains the input {@link MetricConstants}, false otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link StatisticGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getMetrics().contains( input );
        }

        /**
         * Returns a set representation of the enumeration. Contains all elements in {@link StatisticGroup#values()}.
         * 
         * @return a set representation of the elements in this enumeration
         */

        public static Set<StatisticGroup> set()
        {
            return Collections.unmodifiableSet( new HashSet<>( Arrays.asList( StatisticGroup.values() ) ) );
        }

    }

    /**
     * A template associated with one or more scalar values that compose a score.
     */

    public enum ScoreGroup
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

        UNIVARIATE_STATISTIC;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link ScoreGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link ScoreGroup}
         */

        public Set<MetricConstants> getAllComponents()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
            //Remove constants with the same name across MetricConstants and MetricDecompositionGroup
            all.removeIf( a -> Objects.isNull( a.scoreTypeGroup ) || a.name().equals( name() )
                               || !Arrays.asList( a.scoreTypeGroup ).contains( this ) );
            return all;
        }

        /**
         * Returns true if this {@link ScoreGroup} contains the input {@link MetricConstants}, false
         * otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link ScoreGroup} contains the input {@link MetricConstants}, false
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
         * Identifier for forecast error.
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

        TRUE_NEGATIVES;

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

    /**
     * Class for representing missing values associated with different data types.
     */

    public static class MissingValues
    {

        /**
         * Default output for {@link Duration} when missing.
         */

        public static final Duration MISSING_DURATION = null;

        /**
         * Default output for {@link Double} when missing.
         */

        public static final double MISSING_DOUBLE = Double.NaN;

        /**
         * Do not construct.
         */

        private MissingValues()
        {
        }

    }

}
