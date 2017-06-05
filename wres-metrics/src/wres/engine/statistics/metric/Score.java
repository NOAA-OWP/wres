package wres.engine.statistics.metric;

/**
 * Identifies a scoring rule. A score may be an absolute or relative measure. An absolute score is dimensioned, whereas
 * a relative scores is dimensionless. A relative score is known as a skill score. Some scores may be factored into
 * components. Supported decompositions and elements of decompositions are identified by the constants within this
 * class.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface Score
{

    /**
     * <p>
     * Returns true if the input is a supported decomposition identifier, false otherwise. The supported decomposition
     * identifiers are:
     * </p>
     * <ol>
     * <li>{@link MetricConstants#NONE}, indicating no decomposition</li>
     * <li>{@link MetricConstants#CR}, indicating the calibration-refinement decomposition</li>
     * <li>{@link MetricConstants#LBR}, indicating the likelihood base-rate decomposition</li>
     * <li>{@link MetricConstants#CR_AND_LBR}, indicating both the calibration-refinement and likelihood-base-rate
     * decompositions</li>
     * </ol>
     * 
     * @param decompositionID the decomposition identifier to test
     * @return true if the decomposition identifier is valid, false otherwise
     */

    static boolean isSupportedDecompositionID(final int decompositionID)
    {
        switch(decompositionID)
        {
            case MetricConstants.NONE:
            case MetricConstants.CR:
            case MetricConstants.LBR:
            case MetricConstants.CR_AND_LBR:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns true if the score is a relative measure or skill score, false for an absolute measure.
     * 
     * @return true if the score is a skill score
     */

    boolean isSkillScore();

    /**
     * Returns true if the score is decomposable, false otherwise.
     * 
     * @return true if the score is decomposable, false otherwise
     */

    boolean isDecomposable();

    /**
     * <p>
     * Returns the identifier associated with the decomposition. One of:
     * </p>
     * <ol>
     * <li>{@link MetricConstants#NONE}, indicating no decomposition</li>
     * <li>{@link MetricConstants#CR}, indicating the calibration-refinement decomposition</li>
     * <li>{@link MetricConstants#LBR}, indicating the likelihood base-rate decomposition</li>
     * <li>{@link MetricConstants#CR_AND_LBR}, indicating both the calibration-refinement and likelihood-base-rate
     * decompositions</li>
     * </ol>
     * <p>
     * Must return {@link MetricConstants#NONE} when {@link #isDecomposable()} returns false.
     * </p>
     * 
     * @return the type of score decomposition
     */

    int getDecompositionID();

}
