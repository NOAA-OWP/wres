package wres.engine.statistics.metric;

/**
 * Identifies a scoring rule. Where applicable, the score may be factored into components, known as a score
 * decomposition. A score may be an absolute or relative measure. An absolute score is dimensioned, whereas a relative
 * scores is dimensionless. A relative score is known as a skill score.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */

public interface Score
{

    /**
     * Returns true if the score is a relative measure or skill score, false for an absolute measure.
     * 
     * @return true if the score is a skill score
     */

    boolean isSkillScore();

    /**
     * Returns true if the score is decomposable into constituent parts, false otherwise.
     * 
     * @return true if the score is decomposable, false otherwise
     */

    boolean isDecomposable();

}
