package wres.datamodel.metric;

import java.util.EnumSet;
import java.util.Set;

/**
 * Metric constants. The metric identifiers are grouped by metric input/output type, as defined by the
 * {@link MetricGroup}.
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

    BIAS_FRACTION(MetricGroup.SINGLE_VALUED_SCALAR),

    /**
     * Identifier for a Brier Score.
     */

    BRIER_SCORE(MetricGroup.DISCRETE_PROBABILITY_VECTOR),

    /**
     * Identifier for a Brier Skill Score.
     */

    BRIER_SKILL_SCORE(MetricGroup.DISCRETE_PROBABILITY_VECTOR),

    /**
     * Identifier for coefficient of determination.
     */

    COEFFICIENT_OF_DETERMINATION(MetricGroup.SINGLE_VALUED_SCALAR),

    /**
     * Identifier for a Contingency Table.
     */

    CONTINGENCY_TABLE(MetricGroup.MULTICATEGORY_MATRIX),

    /**
     * Identifier for Pearson's product-moment correlation coefficient.
     */

    CORRELATION_PEARSONS(MetricGroup.SINGLE_VALUED_SCALAR),

    /**
     * Identifier for a Critical Success Index.
     */

    CRITICAL_SUCCESS_INDEX(MetricGroup.DICHOTOMOUS_SCALAR),

    /**
     * Identifier for an Equitable Threat Score.
     */

    EQUITABLE_THREAT_SCORE(MetricGroup.DICHOTOMOUS_SCALAR),

    /**
     * Identifier for a Mean Absolute Error.
     */

    MEAN_ABSOLUTE_ERROR(MetricGroup.SINGLE_VALUED_SCALAR),

    /**
     * Identified for a Mean Continuous Ranked Probability Skill Score
     */

    MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,

    /**
     * Identifier for a Mean Error.
     */

    MEAN_ERROR(MetricGroup.SINGLE_VALUED_SCALAR),

    /**
     * Identifier for a Mean Square Error.
     */

    MEAN_SQUARE_ERROR(MetricGroup.SINGLE_VALUED_VECTOR),

    /**
     * Identifier for a Mean Square Error Skill Score.
     */

    MEAN_SQUARE_ERROR_SKILL_SCORE(MetricGroup.SINGLE_VALUED_VECTOR),

    /**
     * Identifier for a Peirce Skill Score.
     */

    PEIRCE_SKILL_SCORE(MetricGroup.DICHOTOMOUS_SCALAR),

    /**
     * Identifier for a Probability Of Detection.
     */

    PROBABILITY_OF_DETECTION(MetricGroup.DICHOTOMOUS_SCALAR),

    /**
     * Identifier for a Probability Of False Detection.
     */

    PROBABILITY_OF_FALSE_DETECTION(MetricGroup.DICHOTOMOUS_SCALAR),
    
    /**
     * Quantile-quantile diagram.
     */

    QUANTILE_QUANTILE_DIAGRAM(MetricGroup.SINGLE_VALUED_MULTIVECTOR),
    
    /**
     * Identifier for the Relative Operating Characteristic.
     */

    RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM(MetricGroup.DISCRETE_PROBABILITY_MULTIVECTOR),
    
    /**
     * Identifier for the Relative Operating Characteristic Score.
     */    
    
    RELATIVE_OPERATING_CHARACTERISTIC_SCORE(MetricGroup.DISCRETE_PROBABILITY_VECTOR),

    /**
     * Identifier for the Reliability Diagram.
     */

    RELIABILITY_DIAGRAM(MetricGroup.DISCRETE_PROBABILITY_MULTIVECTOR),

    /**
     * Identifier for a Root Mean Square Error.
     */

    ROOT_MEAN_SQUARE_ERROR(MetricGroup.SINGLE_VALUED_SCALAR),

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
     * The {@link MetricGroup} or null if the {@link MetricConstant} does not belong to a group.
     */

    private final MetricGroup group;

    /**
     * Default constructor
     */

    private MetricConstants()
    {
        group = null;
    }

    /**
     * Construct with a {@link MetricGroup}.
     * 
     * @param group
     */

    private MetricConstants(MetricGroup group)
    {
        this.group = group;
    }

    /**
     * Returns true if the input {@link MetricGroup} contains the current {@link MetricConstants}, false otherwise.
     * 
     * @param group the {@link MetricGroup}
     * @return true if the input {@link MetricGroup} contains the current {@link MetricConstants}, false otherwise
     */

    public boolean isInGroup(MetricGroup group)
    {
        return this.group == group;
    }

    /**
     * Metric groups.
     */

    public enum MetricGroup
    {

        /**
         * Metrics that consume single-valued inputs and produce scalar outputs.
         */

        SINGLE_VALUED_SCALAR,

        /**
         * Metrics that consume single-valued inputs and produce vector outputs.
         */

        SINGLE_VALUED_VECTOR,
        
        /**
         * Metrics that consume single-valued inputs and produce multi-vector outputs.
         */

        SINGLE_VALUED_MULTIVECTOR,        

        /**
         * Metrics that consume discrete probability inputs and produce vector outputs.
         */

        DISCRETE_PROBABILITY_VECTOR,

        /**
         * Metrics that consume dichotomous inputs and produce scalar outputs.
         */

        DICHOTOMOUS_SCALAR,

        /**
         * Metrics that consume discrete probability inputs and produce outputs that comprise multiple vectors.
         */

        DISCRETE_PROBABILITY_MULTIVECTOR,

        /**
         * Metrics that consume multi-category inputs and produce matrix outputs.
         */

        MULTICATEGORY_MATRIX,
        
        /**
         * Metrics that consume ensemble inputs and produce vector outputs.
         */

        ENSEMBLE_VECTOR,
        
        /**
         * Metrics that consume ensemble inputs and produce multi-vector outputs.
         */

        ENSEMBLE_MULTIVECTOR;        
        
        /**
         * Returns all {@link MetricConstants} associated with the current {@link MetricGroup}.
         * 
         * @return the {@link MetricConstants} associated with the current {@link MetricGroup}
         */

        public Set<MetricConstants> getMetrics()
        {
            Set<MetricConstants> all = EnumSet.allOf(MetricConstants.class);
            all.removeIf(a -> a.group != this);
            return all;
        }

        /**
         * Returns true if this {@link MetricGroup} contains the input {@link MetricConstants}, false otherwise.
         * 
         * @param input the {@link MetricConstants} to test
         * @return true if this {@link MetricGroup} contains the input {@link MetricConstants}, false otherwise
         */

        public boolean contains(MetricConstants input)
        {
            return getMetrics().contains(input);
        }

    }
}
