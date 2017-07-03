package wres.engine.statistics.metric;

import org.junit.Test;

import wres.datamodel.metric.DefaultMetricOutputFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputFactory;

/**
 * Tests the {@link MetricFactory}.
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
        final MetricOutputFactory outF = DefaultMetricOutputFactory.of();
        final MetricFactory metF = MetricFactory.of(outF);        
        metF.ofBrierScore();
        metF.ofBrierScore(MetricConstants.NONE);
        metF.ofBrierSkillScore();
        metF.ofContingencyTable();
        metF.ofCriticalSuccessIndex();
        metF.ofEquitableThreatScore();
        metF.ofMeanAbsoluteError();
        metF.ofMeanError();
        metF.ofMeanSquareError();
        metF.ofMeanSquareErrorSkillScore();
        metF.ofPeirceSkillScore();
        metF.ofPeirceSkillScoreMulti();
        metF.ofProbabilityOfDetection();
        metF.ofProbabilityOfFalseDetection();
        metF.ofRootMeanSquareError();
    }

}
