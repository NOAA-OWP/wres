package wres.engine.statistics.metric;

import org.junit.Test;

/**
 * <p>
 * Tests the {@link MetricFactory}.
 * </p>
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricFactoryTest
{

    /**
     * Tests the methods in {@link MetricFactory}.
     */

    @Test
    public void test1MetricFactory()
    {
        MetricFactory.ofBrierScore();
        MetricFactory.ofBrierScore(MetricConstants.NONE);
        MetricFactory.ofBrierSkillScore();
        MetricFactory.ofContingencyTable();
        MetricFactory.ofCriticalSuccessIndex();
        MetricFactory.ofEquitableThreatScore();
        MetricFactory.ofMeanAbsoluteError();
        MetricFactory.ofMeanError();
        MetricFactory.ofMeanSquareError();
        MetricFactory.ofMeanSquareErrorSkillScore();
        MetricFactory.ofPeirceSkillScore();
        MetricFactory.ofPeirceSkillScoreMulti();
        MetricFactory.ofProbabilityOfDetection();
        MetricFactory.ofProbabilityOfFalseDetection();
        MetricFactory.ofRootMeanSquareError();
    }

}
