package wres.datamodel.metric;

/**
 * Metric constants.
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
    
    BIAS_FRACTION,
    
    /**
     * Identifier for a Brier Score.
     */

    BRIER_SCORE,

    /**
     * Identifier for a Brier Skill Score.
     */

    BRIER_SKILL_SCORE,
    
    /**
     * Identifier for coefficient of determination.
     */

    COEFFICIENT_OF_DETERMINATION,
    
    /**
     * Identifier for a Contingency Table.
     */

    CONTINGENCY_TABLE,

    /**
     * Identifier for Pearson's product-moment correlation coefficient.
     */
    
    CORRELATION_PEARSONS,
    
    /**
     * Identifier for a Critical Success Index.
     */

    CRITICAL_SUCCESS_INDEX,

    /**
     * Identifier for an Equitable Threat Score.
     */

    EQUITABLE_THREAT_SCORE,

    /**
     * Identifier for a Mean Absolute Error.
     */

    MEAN_ABSOLUTE_ERROR,

    /**
     * Identified for a Mean Continuous Ranked Probability Skill Score 
     */
    
    MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
    
    /**
     * Identifier for a Mean Error.
     */

    MEAN_ERROR,

    /**
     * Identifier for a Mean Square Error.
     */

    MEAN_SQUARE_ERROR,

    /**
     * Identifier for a Mean Square Error Skill Score.
     */

    MEAN_SQUARE_ERROR_SKILL_SCORE,

    /**
     * Identifier for a Peirce Skill Score.
     */

    PEIRCE_SKILL_SCORE,

    /**
     * Identifier for a Probability Of Detection.
     */

    PROBABILITY_OF_DETECTION,

    /**
     * Identifier for a Probability Of False Detection.
     */

    PROBABILITY_OF_FALSE_DETECTION,
    
    /**
     * Identifier for the Relative Operating Characteristic.
     */

    RELATIVE_OPERATING_CHARACTERISTIC,
    
    /**
     * Identifier for a Root Mean Square Error.
     */

    ROOT_MEAN_SQUARE_ERROR,

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

    SAMPLE_SIZE

}
