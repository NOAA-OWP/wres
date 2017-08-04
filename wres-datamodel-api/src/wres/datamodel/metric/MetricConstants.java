package wres.datamodel.metric;

import java.util.EnumSet;
import java.util.Set;

/**
 * Metric constants. The metric identifiers are grouped by metric input/output type, as defined by the
 * {@link MetricInputGroup} and {@link MetricOutputGroup}, respectively.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public enum MetricConstants
{

    /**
     * Identifier for fractional bias or relative mean error.
     */

    BIAS_FRACTION(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR),

    /**
     * Identifier for a Brier Score.
     */

    BRIER_SCORE(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR),

    /**
     * Identifier for a Brier Skill Score.
     */

    BRIER_SKILL_SCORE(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR),

    /**
     * Identifier for coefficient of determination.
     */

    COEFFICIENT_OF_DETERMINATION(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR),

    /**
     * Identifier for a Contingency Table.
     */

    CONTINGENCY_TABLE(MetricInputGroup.MULTICATEGORY, MetricOutputGroup.MATRIX),

    /**
     * Identifier for Pearson's product-moment correlation coefficient.
     */

    CORRELATION_PEARSONS(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR),

    /**
     * Identifier for a Critical Success Index.
     */

    CRITICAL_SUCCESS_INDEX(MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR),

    /**
     * Identifier for an Equitable Threat Score.
     */

    EQUITABLE_THREAT_SCORE(MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR),

    /**
     * Identifier for a Mean Absolute Error.
     */

    MEAN_ABSOLUTE_ERROR(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR),

    /**
     * Identified for a Mean Continuous Ranked Probability Skill Score
     */

    MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,

    /**
     * Identifier for a Mean Error.
     */

    MEAN_ERROR(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR),

    /**
     * Identifier for a Mean Square Error.
     */

    MEAN_SQUARE_ERROR(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR),

    /**
     * Identifier for a Mean Square Error Skill Score.
     */

    MEAN_SQUARE_ERROR_SKILL_SCORE(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.VECTOR),

    /**
     * Identifier for a Peirce Skill Score.
     */

    PEIRCE_SKILL_SCORE(MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR),

    /**
     * Identifier for a Probability Of Detection.
     */

    PROBABILITY_OF_DETECTION(MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR),

    /**
     * Identifier for a Probability Of False Detection.
     */

    PROBABILITY_OF_FALSE_DETECTION(MetricInputGroup.DICHOTOMOUS, MetricOutputGroup.SCALAR),

    /**
     * Quantile-quantile diagram.
     */

    QUANTILE_QUANTILE_DIAGRAM(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.MULTIVECTOR),

    /**
     * Identifier for the Relative Operating Characteristic.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR),

    /**
     * Identifier for the Relative Operating Characteristic Score.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_SCORE(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.VECTOR),

    /**
     * Identifier for the Reliability Diagram.
     */

    RELIABILITY_DIAGRAM(MetricInputGroup.DISCRETE_PROBABILITY, MetricOutputGroup.MULTIVECTOR),

    /**
     * Identifier for a Root Mean Square Error.
     */

    ROOT_MEAN_SQUARE_ERROR(MetricInputGroup.SINGLE_VALUED, MetricOutputGroup.SCALAR),

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
     * Identifier for a decomposition into Type-II conditional bias - discrimination + sharpness, comprising the overall
     * score followed by the three components in that order.
     */

    LBR,

    /**
     * Identifier for the score and components of both the CR and LBR factorizations, as well as several mixed
     * components: The overall score Reliability Resolution Uncertainty Type-II conditional bias Discrimination
     * Sharpness And further components: Score | Observed to occur Score | Observed to not occur And further components:
     * Type-II conditional bias | Observed to occur Type-II conditional bias | Observed to not occur Discrimination |
     * Observed to occur Discrimination | Observed to not occur
     */

    CR_AND_LBR,

    /**
     * Identifier for the main component of a metric, such as the overall score in a score decomposition.
     */

    MAIN,

    /**
     * Identifier for the reliability component of a score decomposition.
     */

    RELIABILITY,

    /**
     * Identifier for the resolution component of a score decomposition.
     */

    RESOLUTION,

    /**
     * Identifier for the uncertainty component of a score decomposition.
     */

    UNCERTAINTY,

    /**
     * Identifier for the potential score value (perfect reliability).
     */

    POTENTIAL,

    /**
     * Identifier for the Type-II conditional bias component of a score decomposition.
     */

    TYPE_II_BIAS,

    /**
     * Identifier for the discrimination component of a score decomposition.
     */

    DISCRIMINATION,

    /**
     * Identifier for the sharpness component of a score decomposition.
     */

    SHARPNESS,

    /**
     * Identifier for the sample size component of a metric.
     */

    SAMPLE_SIZE,

    /**
     * Identifier for a forecast probability.
     */

    FORECAST_PROBABILITY,

    /**
     * Identifier for the conditional observed probability of an event, given the forecast probability.
     */

    OBSERVED_GIVEN_FORECAST_PROBABILITY,

    /**
     * Identifier for predicted quantiles.
     */

    PREDICTED_QUANTILES,

    /**
     * Identifier for observed quantiles.
     */

    OBSERVED_QUANTILES;

    /**
     * The {@link MetricInputGroup} or null if the {@link MetricConstant} does not belong to a group.
     */

    private final MetricInputGroup inGroup;

    /**
     * The {@link MetricOutputGroup} or null if the {@link MetricConstant} does not belong to a group.
     */

    private final MetricOutputGroup outGroup;

    /**
     * Default constructor
     */

    private MetricConstants()
    {
        inGroup = null;
        outGroup = null;
    }

    /**
     * Construct with a {@link MetricGroup}.
     * 
     * @param inputGroup the input group
     * @param outputGroup the output group
     */

    private MetricConstants(MetricInputGroup inGroup, MetricOutputGroup outGroup)
    {
        this.inGroup = inGroup;
        this.outGroup = outGroup;
    }

    /**
     * Returns true if the input {@link MetricInputGroup} contains the current {@link MetricConstants}, false otherwise.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @return true if the input {@link MetricInputGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup(MetricInputGroup inGroup)
    {
        return this.inGroup == inGroup;
    }

    /**
     * Returns true if the input {@link MetricOutputGroup} contains the current {@link MetricConstants}, false
     * otherwise.
     * 
     * @param outGroup the {@link MetricOutputGroup}
     * @return true if the input {@link MetricOutputGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup(MetricOutputGroup outGroup)
    {
        return this.outGroup == outGroup;
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

    public boolean isInGroup(MetricInputGroup inGroup, MetricOutputGroup outGroup)
    {
        return isInGroup(inGroup) && isInGroup(outGroup);
    }

    /**
     * Returns all {@link MetricConstants} associated with the specified {@link MetricInputGroup} and
     * {@link MetricOutputGroup}.
     * 
     * @param inGroup the {@link MetricInputGroup}
     * @param outGroup the {@link MetricOutputGroup}
     * @return the {@link MetricConstants} associated with the current {@link MetricInputGroup}
     */

    public static Set<MetricConstants> getMetrics(MetricInputGroup inGroup, MetricOutputGroup outGroup)
    {
        Set<MetricConstants> all = EnumSet.allOf(MetricConstants.class);
        all.removeIf(a -> a.inGroup != inGroup || a.outGroup != outGroup);
        return all;
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
            Set<MetricConstants> all = EnumSet.allOf(MetricConstants.class);
            all.removeIf(a -> a.inGroup != this);
            return all;
        }

        /**
         * Returns true if this {@link MetricInputGroup} contains the input {@link MetricConstants}, false otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricInputGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains(MetricConstants input)
        {
            return getMetrics().contains(input);
        }

    }

    /**
     * Type of metric output.
     */

    public enum MetricOutputGroup
    {

        /**
         * Metrics that produce a scalar output.
         */

        SCALAR,

        /**
         * Metrics that produce a vector of outputs.
         */

        VECTOR,

        /**
         * Metrics that produce multiple vectors of outputs.
         */

        MULTIVECTOR,

        /**
         * Metrics that produce matrix outputs.
         */

        MATRIX;

        /**
         * Returns all {@link MetricConstants} associated with the current {@link MetricOutputGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link MetricOutputGroup}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf(MetricConstants.class);
            all.removeIf(a -> a.outGroup != this);
            return all;
        }

        /**
         * Returns true if this {@link MetricOutputGroup} contains the input {@link MetricConstants}, false otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricOutputGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains(MetricConstants input)
        {
            return getMetrics().contains(input);
        }

    }

}
