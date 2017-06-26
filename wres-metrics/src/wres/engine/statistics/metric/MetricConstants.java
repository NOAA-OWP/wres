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
     * Identifier for the {@link BrierScore}.
     */

    public static final int BRIER_SCORE = 101;

    /**
     * Identifier for the {@link BrierSkillScore}.
     */

    public static final int BRIER_SKILL_SCORE = 102;

    /**
     * Identifier for the {@link ContingencyTable}.
     */

    public static final int CONTINGENCY_TABLE = 103;

    /**
     * Identifier for the {@link CriticalSuccessIndex}.
     */

    public static final int CRITICAL_SUCCESS_INDEX = 104;

    /**
     * Identifier for the {@link EquitableThreatScore}.
     */

    public static final int EQUITABLE_THREAT_SCORE = 105;

    /**
     * Identifier for the {@link MeanAbsoluteError}.
     */

    public static final int MEAN_ABSOLUTE_ERROR = 106;

    /**
     * Identifier for the {@link MeanError}.
     */

    public static final int MEAN_ERROR = 107;

    /**
     * Identifier for the {@link MeanSquareError}.
     */

    public static final int MEAN_SQUARE_ERROR = 108;

    /**
     * Identifier for the {@link MeanSquareErrorSkillScore}.
     */

    public static final int MEAN_SQUARE_ERROR_SKILL_SCORE = 109;

    /**
     * Identifier for the {@link PeirceSkillScore}.
     */

    public static final int PEIRCE_SKILL_SCORE = 110;

    /**
     * Identifier for the {@link ProbabilityOfDetection}.
     */

    public static final int PROBABILITY_OF_DETECTION = 111;

    /**
     * Identifier for the {@link ProbabilityOfFalseDetection}.
     */

    public static final int PROBABILITY_OF_FALSE_DETECTION = 112;

    /**
     * Identifier for the {@link RootMeanSquareError}.
     */

    public static final int ROOT_MEAN_SQUARE_ERROR = 113;

    /**
     * Indicator for no decomposition.
     */

    public static final int NONE = 1001;

    /**
     * Identifier for a Calibration-Refinement (CR) factorization into reliability - resolution + uncertainty,
     * comprising the overall score followed by the three components in that order.
     */

    public static final int CR = 1002;

    /**
     * Identifier for a decomposition into Type-II conditional bias - discrimination + sharpness, comprising the overall
     * score followed by the three components in that order.
     */

    public static final int LBR = 1003;

    /**
     * Identifier for the score and components of both the CR and LBR factorizations, as well as several mixed
     * components: The overall score Reliability Resolution Uncertainty Type-II conditional bias Discrimination
     * Sharpness And further components: Score | Observed to occur Score | Observed to not occur And further components:
     * Type-II conditional bias | Observed to occur Type-II conditional bias | Observed to not occur Discrimination |
     * Observed to occur Discrimination | Observed to not occur
     */

    public static final int CR_AND_LBR = 1004;

    /**
     * The default decomposition.
     */

    public static final int DEFAULT = NONE;

    /**
     * Identifier for the main component of a metric, such as the overall score in a score decomposition.
     */

    public static final int MAIN = 2001;

    /**
     * Identifier for the reliability component of a score decomposition.
     */

    public static final int RELIABILITY = 2002;

    /**
     * Identifier for the resolution component of a score decomposition.
     */

    public static final int RESOLUTION = 2003;

    /**
     * Identifier for the uncertainty component of a score decomposition.
     */

    public static final int UNCERTAINTY = 2004;

    /**
     * Identifier for the potential score value (perfect reliability).
     */

    public static final int POTENTIAL = 2005;

    /**
     * Identifier for the Type-II conditional bias component of a score decomposition.
     */

    public static final int TYPE_II_BIAS = 2006;

    /**
     * Identifier for the discrimination component of a score decomposition.
     */

    public static final int DISCRIMINATION = 2007;

    /**
     * Identifier for the sharpness component of a score decomposition.
     */

    public static final int SHARPNESS = 2008;

    /**
     * Identifier for the sample size component of a metric.
     */

    public static final int SAMPLE_SIZE = 2009;

    /**
     * Returns a metric name for a prescribed metric identifier from this class.
     * 
     * @param identifier the metric identifier
     * @return a metric name for the input identifier
     */

    public static String getMetricName(final int identifier)
    {
        switch(identifier)
        {
            case BRIER_SCORE:
                return "BRIER SCORE";
            case BRIER_SKILL_SCORE:
                return "BRIER SKILL SCORE";
            case CONTINGENCY_TABLE:
                return "CONTINGENCY TABLE";
            case CRITICAL_SUCCESS_INDEX:
                return "CRITICAL SUCCESS INDEX";
            case EQUITABLE_THREAT_SCORE:
                return "EQUITABLE THREAT SCORE";
            case MEAN_ABSOLUTE_ERROR:
                return "MEAN_ABSOLUTE_ERROR";
            case MEAN_ERROR:
                return "MEAN_ERROR";
            case MEAN_SQUARE_ERROR:
                return "MEAN SQUARE ERROR";
            case MEAN_SQUARE_ERROR_SKILL_SCORE:
                return "MEAN SQUARE ERROR SKILL SCORE";
            case PEIRCE_SKILL_SCORE:
                return "PEIRCE SKILL SCORE";
            case PROBABILITY_OF_DETECTION:
                return "PROBABILITY OF DETECTION";
            case PROBABILITY_OF_FALSE_DETECTION:
                return "PROBABILITY OF FALSE DETECTION";
            case ROOT_MEAN_SQUARE_ERROR:
                return "ROOT MEAN SQUARE ERROR";
            default:
                throw new MetricException("Unable to determine the metric name from the prescribed identifier '"
                    + identifier + "'.");
        }
    }

    /**
     * Returns the name associated with a prescribed metric component from this class, such as a score component.
     * 
     * @param identifier the metric component identifier
     * @return a metric component name for the input identifier
     */

    public static String getMetricComponentName(final int identifier)
    {
        switch(identifier)
        {
            case MAIN:
                return "MAIN OUTPUT";
            case RELIABILITY:
                return "RELIABILITY";
            case RESOLUTION:
                return "RESOLUTION";
            case UNCERTAINTY:
                return "UNCERTAINTY";
            case TYPE_II_BIAS:
                return "TYPE II CONDITIONAL BIAS";
            case DISCRIMINATION:
                return "DISCRIMINATION";
            case SHARPNESS:
                return "SHARPNESS";
            case SAMPLE_SIZE:
                return "SAMPLE_SIZE";
            default:
                throw new MetricException("Unable to determine the metric component name from the prescribed identifier '"
                    + identifier + "'.");
        }
    }

    /**
     * Prevent construction.
     */

    private MetricConstants()
    {
    }
}
