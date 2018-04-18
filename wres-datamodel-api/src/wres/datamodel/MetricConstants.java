package wres.datamodel;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import wres.datamodel.outputs.BoxPlotOutput;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.datamodel.outputs.DurationScoreOutput;
import wres.datamodel.outputs.MatrixOutput;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.datamodel.outputs.PairedOutput;

/**
 * Metric constants. The metric identifiers are grouped by metric input/output type, as defined by the
 * {@link MetricInputGroup} and {@link MetricOutputGroup}, respectively.
 * 
 * @author james.brown@hydrosolved.com
 */

public enum MetricConstants
{

    /**
     * Identifier for fractional bias or relative mean error.
     */

    BIAS_FRACTION( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Brier Score.
     */

    BRIER_SCORE( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Brier Skill Score.
     */

    BRIER_SKILL_SCORE( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a box plot of errors by observed value.
     */

    BOX_PLOT_OF_ERRORS_BY_OBSERVED_VALUE( MetricInputGroup.ENSEMBLE, MetricOutputGroup.BOXPLOT ),

    /**
     * Identifier for a box plot of errors by forecast value.
     */

    BOX_PLOT_OF_ERRORS_BY_FORECAST_VALUE( MetricInputGroup.ENSEMBLE, MetricOutputGroup.BOXPLOT ),

    /**
     * Identifier for coefficient of determination.
     */

    COEFFICIENT_OF_DETERMINATION( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Continuous Ranked Probability Score
     */

    CONTINUOUS_RANKED_PROBABILITY_SCORE( MetricInputGroup.ENSEMBLE, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Continuous Ranked Probability Skill Score
     */

    CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE( MetricInputGroup.ENSEMBLE, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Contingency Table.
     */

    CONTINGENCY_TABLE( new MetricInputGroup[] { MetricInputGroup.DICHOTOMOUS,
                                                MetricInputGroup.MULTICATEGORY },
            MetricOutputGroup.MATRIX ),

    /**
     * Identifier for Pearson's product-moment correlation coefficient.
     */

    PEARSON_CORRELATION_COEFFICIENT( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Threat Score.
     */

    THREAT_SCORE( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for an Equitable Threat Score.
     */

    EQUITABLE_THREAT_SCORE( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Frequency Bias.
     */

    FREQUENCY_BIAS( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for an Index of Agreement.
     */

    INDEX_OF_AGREEMENT( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the Kling-Gupta Efficiency index.
     */

    KLING_GUPTA_EFFICIENCY( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Absolute Error.
     */

    MEAN_ABSOLUTE_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Error.
     */

    MEAN_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Square Error.
     */

    MEAN_SQUARE_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Mean Square Error Skill Score.
     */

    MEAN_SQUARE_ERROR_SKILL_SCORE( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Peirce Skill Score.
     */

    PEIRCE_SKILL_SCORE( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Probability Of Detection.
     */

    PROBABILITY_OF_DETECTION( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for a Probability Of False Detection.
     */

    PROBABILITY_OF_FALSE_DETECTION( MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Quantile-quantile diagram.
     */

    QUANTILE_QUANTILE_DIAGRAM( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR ),

    /**
     * Identifier for the Rank Histogram.
     */

    RANK_HISTOGRAM( MetricInputGroup.ENSEMBLE, MetricOutputGroup.MULTIVECTOR ),

    /**
     * Identifier for the Relative Operating Characteristic.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ),

    /**
     * Identifier for the Relative Operating Characteristic Score.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_SCORE( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the Reliability Diagram.
     */

    RELIABILITY_DIAGRAM( MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR ),

    /**
     * Identifier for a Root Mean Square Error.
     */

    ROOT_MEAN_SQUARE_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the sample size.
     */

    SAMPLE_SIZE( new MetricInputGroup[] { MetricInputGroup.SINGLE_VALUED,
                                          MetricInputGroup.ENSEMBLE },
            MetricOutputGroup.DOUBLE_SCORE, ScoreOutputGroup.UNIVARIATE_STATISTIC ),
    
    /**
     * Identifier for a Sum of Square Error.
     */

    SUM_OF_SQUARE_ERROR( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),    

    /**
     * Identifier for the Volumetric Efficiency.
     */

    VOLUMETRIC_EFFICIENCY( MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.DOUBLE_SCORE ),

    /**
     * Identifier for the Time-to-Peak Error.
     */

    TIME_TO_PEAK_ERROR( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, MetricOutputGroup.PAIRED ),

    /**
     * Identifier for a statistic derived from the Time-to-Peak Error.
     */

    TIME_TO_PEAK_ERROR_STATISTIC( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, MetricOutputGroup.DURATION_SCORE ),

    /**
     * Identifier for the Time-to-Peak Relative Error.
     */

    TIME_TO_PEAK_RELATIVE_ERROR( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, MetricOutputGroup.PAIRED ),

    /**
     * Identifier for a statistic derived from the Time-to-Peak Relative Error.
     */

    TIME_TO_PEAK_RELATIVE_ERROR_STATISTIC( MetricInputGroup.SINGLE_VALUED_TIME_SERIES, MetricOutputGroup.DURATION_SCORE ),

    /**
     * Mean statistic.
     */

    MEAN( ScoreOutputGroup.UNIVARIATE_STATISTIC ),

    /**
     * Median statistic.
     */

    MEDIAN( ScoreOutputGroup.UNIVARIATE_STATISTIC ),

    /**
     * Standard deviation statistic.
     */

    STANDARD_DEVIATION( ScoreOutputGroup.UNIVARIATE_STATISTIC ),

    /**
     * Minimum statistic.
     */

    MINIMUM( ScoreOutputGroup.UNIVARIATE_STATISTIC ),

    /**
     * Maximum statistic.
     */

    MAXIMUM( ScoreOutputGroup.UNIVARIATE_STATISTIC ),

    /**
     * Mean absolute statistic.
     */

    MEAN_ABSOLUTE( ScoreOutputGroup.UNIVARIATE_STATISTIC ),

    /**
     * Indicator for no decomposition.
     */

    NONE( ScoreOutputGroup.NONE ),

    /**
     * Identifier for a Calibration-Refinement (CR) factorization.
     */

    CR( ScoreOutputGroup.CR ),

    /**
     * Identifier for a Calibration-Refinement (CR) factorization, together with an additional potential score
     * component.
     */

    CR_POT( ScoreOutputGroup.CR_POT ),

    /**
     * Identifier for a Likelihood-Base-Rate (LBR) factorization.
     */

    LBR( ScoreOutputGroup.LBR ),

    /**
     * Identifier for the score and components of both the CR and LBR factorizations.
     */

    CR_AND_LBR( ScoreOutputGroup.CR_AND_LBR ),

    /**
     * Identifier for the main component of a metric, such as the overall score in a score decomposition.
     */

    MAIN( ScoreOutputGroup.NONE, ScoreOutputGroup.CR, ScoreOutputGroup.CR_POT, ScoreOutputGroup.LBR, ScoreOutputGroup.CR_AND_LBR ),

    /**
     * Identifier for the reliability component of a score decomposition.
     */

    RELIABILITY( ScoreOutputGroup.CR, ScoreOutputGroup.CR_POT, ScoreOutputGroup.CR_AND_LBR ),

    /**
     * Identifier for the resolution component of a score decomposition.
     */

    RESOLUTION( ScoreOutputGroup.CR, ScoreOutputGroup.CR_POT, ScoreOutputGroup.CR_AND_LBR ),

    /**
     * Identifier for the uncertainty component of a score decomposition.
     */

    UNCERTAINTY( ScoreOutputGroup.CR, ScoreOutputGroup.CR_POT, ScoreOutputGroup.CR_AND_LBR ),

    /**
     * Identifier for the potential score value (perfect reliability).
     */

    POTENTIAL( ScoreOutputGroup.CR_POT ),

    /**
     * Identifier for the Type-II conditional bias component of a score decomposition.
     */

    TYPE_II_CONDITIONAL_BIAS( ScoreOutputGroup.LBR, ScoreOutputGroup.CR_AND_LBR ),

    /**
     * Identifier for the discrimination component of a score decomposition.
     */

    DISCRIMINATION( ScoreOutputGroup.LBR, ScoreOutputGroup.CR_AND_LBR ),

    /**
     * Identifier for the sharpness component of a score decomposition.
     */

    SHARPNESS( ScoreOutputGroup.LBR, ScoreOutputGroup.CR_AND_LBR );

    /**
     * The {@link MetricInputGroup} or null if the {@link MetricConstants} does not belong to a group.
     */

    private final MetricInputGroup[] inGroup;

    /**
     * The {@link MetricOutputGroup} or null if the {@link MetricConstants} does not belong to a group.
     */

    private final MetricOutputGroup outGroup;

    /**
     * The {@link ScoreOutputGroup} to which this {@link MetricConstants} belongs or null if the
     * {@link MetricConstants} does not belong to a {@link ScoreOutputGroup}.
     */

    private final ScoreOutputGroup[] scoreTypeGroup;

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
     * Construct with a {@link MetricInputGroup} and a {@link MetricOutputGroup}.
     * 
     * @param inputGroup the input group
     * @param outputGroup the output group
     */

    private MetricConstants( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        this( new MetricInputGroup[] { inGroup }, outGroup, (ScoreOutputGroup[]) null );
    }

    /**
     * Construct with multiple {@link MetricInputGroup} and a {@link MetricOutputGroup}.
     * 
     * @param inGroups the input groups
     * @param secondGroup the second input group
     * @param outputGroup the output group
     */

    private MetricConstants( MetricInputGroup[] inGroups,
                             MetricOutputGroup outGroup,
                             ScoreOutputGroup... scoreTypeGroup )
    {
        this.inGroup = inGroups;
        this.outGroup = outGroup;
        this.scoreTypeGroup = scoreTypeGroup;
    }

    /**
     * Construct with a varargs of {@link ScoreOutputGroup}.
     * 
     * @param decGroup the decomposition groups to which the {@link MetricConstants} belongs
     */

    private MetricConstants( ScoreOutputGroup... decGroup )
    {
        this.scoreTypeGroup = decGroup;
        this.inGroup = null;
        this.outGroup = null;
    }

    /**
     * Returns true if the input {@link MetricInputGroup} contains the current {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @return true if the input {@link MetricInputGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( MetricInputGroup inGroup )
    {
        return Arrays.asList( this.inGroup ).contains( inGroup );
    }

    /**
     * Returns true if the input {@link MetricOutputGroup} contains the current {@link MetricConstants}, false
     * otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if the input {@link MetricOutputGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( MetricOutputGroup outGroup )
    {
        return this.outGroup == outGroup;
    }

    /**
     * Returns true if the input {@link ScoreOutputGroup} contains the current {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link ScoreOutputGroup}
     * @return true if the input {@link ScoreOutputGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( ScoreOutputGroup inGroup )
    {
        return Arrays.asList( this.scoreTypeGroup ).contains( inGroup );
    }

    /**
     * Returns true if the input {@link MetricInputGroup} and {@link MetricOutputGroup} both contain the current
     * {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if the input {@link MetricInputGroup} and {@link MetricOutputGroup} and both contain the current
     *         {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        return isInGroup( inGroup ) && isInGroup( outGroup );
    }

    /**
     * Returns the {@link MetricOutputGroup} associated with the {@link MetricConstants}.
     * 
     * @return the {@link MetricOutputGroup}.
     */

    public MetricOutputGroup getMetricOutputGroup()
    {
        return outGroup;
    }

    /**
     * Returns all metric components in the {@link ScoreOutputGroup} with which this constant is associated or
     * null if none is defined.
     * 
     * @return the components in the {@link ScoreOutputGroup} or null
     */

    public Set<MetricConstants> getAllComponents()
    {
        return Objects.isNull( scoreTypeGroup ) ? null : scoreTypeGroup[0].getAllComponents();
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link MetricInputGroup} and
     * {@link MetricOutputGroup}.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return the {@link MetricConstants} associated with the current {@link MetricInputGroup}
     */

    public static Set<MetricConstants> getMetrics( MetricInputGroup inGroup, MetricOutputGroup outGroup )
    {
        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.inGroup ) || !Arrays.asList( a.inGroup ).contains( inGroup )
                           || a.outGroup != outGroup );
        return Collections.unmodifiableSet( all );
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link MetricInputGroup}.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @return the {@link MetricConstants} associated with the current {@link MetricInputGroup}
     */

    public static Set<MetricConstants> getMetrics( MetricInputGroup inGroup )
    {
        Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
        all.removeIf( a -> Objects.isNull( a.inGroup ) || !Arrays.asList( a.inGroup ).contains( inGroup ) );
        return Collections.unmodifiableSet( all );
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link MetricOutputGroup}.
     * 
     * @param outGroup the {@link MetricOutputGroup}
     * @return the {@link MetricConstants} associated with the current {@link MetricOutputGroup}
     */

    public static Set<MetricConstants> getMetrics( MetricOutputGroup outGroup )
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
     * Type of metric input.
     */

    public enum MetricInputGroup
    {

        /**
         * Metrics that consume single-valued inputs.
         */

        SINGLE_VALUED,

        /**
         * Metrics that consume single-valued time-series inputs.
         */

        SINGLE_VALUED_TIME_SERIES,

        /**
         * Metrics that consume discrete probability inputs.
         */

        DISCRETE_PROBABILITY,

        /**
         * Metrics that consume dichotomous inputs.
         */

        DICHOTOMOUS,

        /**
         * Metrics that consume multi-category inputs.
         */

        MULTICATEGORY,

        /**
         * Metrics that consume ensemble inputs.
         */

        ENSEMBLE;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link MetricInputGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link MetricInputGroup}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
            all.removeIf( a -> Objects.isNull( a.inGroup ) || !Arrays.asList( a.inGroup ).contains( this ) );
            return all;
        }

        /**
         * Returns true if this {@link MetricInputGroup} contains the input {@link MetricConstants}, false otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricInputGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getMetrics().contains( input );
        }

        /**
         * Returns a set representation of the enumeration. Contains all elements in {@link MetricInputGroup#values()}.
         * 
         * @return a set representation of the elements in this enumeration
         */

        public static Set<MetricInputGroup> set()
        {
            return Collections.unmodifiableSet( new HashSet<>( Arrays.asList( MetricInputGroup.values() ) ) );
        }

    }

    /**
     * Type of metric output.
     */

    public enum MetricOutputGroup
    {

        /**
         * Metrics that produce a {@link MultiVectorOutput}.
         */

        MULTIVECTOR,

        /**
         * Metrics that produce a {@link MatrixOutput}.
         */

        MATRIX,

        /**
         * Metrics that produce a {@link BoxPlotOutput}.
         */

        BOXPLOT,

        /**
         * Metrics that produce a {@link PairedOutput}.
         */

        PAIRED,

        /**
         * Metrics that produce a {@link DoubleScoreOutput}.
         */

        DOUBLE_SCORE,

        /**
         * Metrics that produce a {@link DurationScoreOutput}.
         */

        DURATION_SCORE;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link MetricOutputGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link MetricOutputGroup}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf( MetricConstants.class );
            all.removeIf( a -> a.outGroup != this );
            return all;
        }

        /**
         * Returns true if this {@link MetricOutputGroup} contains the input {@link MetricConstants}, false otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricOutputGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains( MetricConstants input )
        {
            return getMetrics().contains( input );
        }

        /**
         * Returns a set representation of the enumeration. Contains all elements in {@link MetricOutputGroup#values()}.
         * 
         * @return a set representation of the elements in this enumeration
         */

        public static Set<MetricOutputGroup> set()
        {
            return Collections.unmodifiableSet( new HashSet<>( Arrays.asList( MetricOutputGroup.values() ) ) );
        }

    }

    /**
     * A template associated with one or more scalar values that compose a score.
     */

    public enum ScoreOutputGroup
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
         * Returns all {@link MetricConstants} associated with the current {@link ScoreOutputGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link ScoreOutputGroup}
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
         * Returns true if this {@link ScoreOutputGroup} contains the input {@link MetricConstants}, false
         * otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link ScoreOutputGroup} contains the input {@link MetricConstants}, false
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
