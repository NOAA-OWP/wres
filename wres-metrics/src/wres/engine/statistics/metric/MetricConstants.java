package wres.engine.statistics.metric;

/**
 * Metric constants.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public final class MetricConstants
{
    /**
     * Indicator for no decomposition.
     */

    static final int NONE = 1001;

    /**
     * Identifier for a Calibration-Refinement (CR) factorization into reliability - resolution + uncertainty,
     * comprising the overall score followed by the three components in that order.
     */

    static final int CR = 1002;

    /**
     * Identifier for a decomposition into Type-II conditional bias - discrimination + sharpness, comprising the overall
     * score followed by the three components in that order.
     */

    static final int LBR = 1003;

    /**
     * Identifier for the score and components of both the CR and LBR factorizations, as well as several mixed
     * components: The overall score Reliability Resolution Uncertainty Type-II conditional bias Discrimination
     * Sharpness And further components: Score | Observed to occur Score | Observed to not occur And further components:
     * Type-II conditional bias | Observed to occur Type-II conditional bias | Observed to not occur Discrimination |
     * Observed to occur Discrimination | Observed to not occur
     */

    static final int CR_AND_LBR = 1004;

    /**
     * The default decomposition.
     */

    static final int DEFAULT = NONE;

    /**
     * Identifier for the overall score in a score decomposition.
     */

    static final int OVERALL_SCORE = 2001;

    /**
     * Identifier for the reliability component of a score decomposition.
     */

    static final int RELIABILITY = 2002;

    /**
     * Identifier for the resolution component of a score decomposition.
     */

    static final int RESOLUTION = 2003;

    /**
     * Identifier for the uncertainty component of a score decomposition.
     */

    static final int UNCERTAINTY = 2004;

    /**
     * Identifier for the potential score value (perfect reliability).
     */

    static final int POTENTIAL = 2005;

    /**
     * Identifier for the Type-II conditional bias component of a score decomposition.
     */

    static final int TYPE_II_BIAS = 2006;

    /**
     * Identifier for the discrimination component of a score decomposition.
     */

    static final int DISCRIMINATION = 2007;

    /**
     * Identifier for the sharpness component of a score decomposition.
     */

    static final int SHARPNESS = 2008;

    /**
     * Prevent construction.
     */

    private MetricConstants()
    {
    }
}
