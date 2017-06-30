package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * Tests the {@link Score}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ScoreTest
{

    /**
     * Tests the {@link Score#isSupportedDecompositionID(int)}.
     */

    @Test
    public void test1Score()
    {
        assertTrue("One of the score decomposition identifiers was unexpectedly invalid.",
                   Score.isSupportedDecompositionID(MetricConstants.CR));
        assertTrue("One of the score decomposition identifiers was unexpectedly invalid.",
                   Score.isSupportedDecompositionID(MetricConstants.LBR));
        assertTrue("One of the score decomposition identifiers was unexpectedly invalid.",
                   Score.isSupportedDecompositionID(MetricConstants.CR_AND_LBR));
        assertTrue("An invalid score decomposition identifier was unexpectedly returned as valid.",
                   !Score.isSupportedDecompositionID(MetricConstants.DISCRIMINATION));
    }
}
