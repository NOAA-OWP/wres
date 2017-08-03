package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputForProjectByThreshold;
import wres.datamodel.metric.MetricOutputMapByMetric;
import wres.datamodel.metric.ScalarOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.datamodel.metric.Threshold.Condition;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessorSingleValuedPairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorSingleValuedPairsTest
{

    private final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * Tests the construction of a {@link MetricProcessorSingleValuedPairs} and application of
     * {@link MetricProcessorSingleValuedPairs#apply(wres.datamodel.metric.SingleValuedPairs)} to configuration obtained
     * from testinput/metricProcessorSingleValuedPairsTest/test1ApplyNoThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getSingleValuedPairsFour()}.
     */

    @Test
    public void test1ApplyNoThresholds()
    {
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test1ApplyNoThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from(Paths.get(configPath)).getProjectConfig();
            MetricProcessorSingleValuedPairs processor =
                                                       (MetricProcessorSingleValuedPairs)MetricProcessor.of(dataFactory,
                                                                                                            config);
            SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
            MetricOutputForProjectByThreshold results = processor.apply(pairs);
            MetricOutputMapByMetric<ScalarOutput> testMe = results.getScalarOutput()
                                                                  .get(dataFactory.getThreshold(Double.NEGATIVE_INFINITY,
                                                                                                Condition.GREATER));
            //Test contents
            assertTrue("Unexpected difference in " + MetricConstants.BIAS_FRACTION,
                       testMe.get(MetricConstants.BIAS_FRACTION).getData().equals(-1.6666666666666667));
            assertTrue("Unexpected difference in " + MetricConstants.COEFFICIENT_OF_DETERMINATION,
                       testMe.get(MetricConstants.COEFFICIENT_OF_DETERMINATION).getData().equals(1.0));
            assertTrue("Unexpected difference in " + MetricConstants.CORRELATION_PEARSONS,
                       testMe.get(MetricConstants.CORRELATION_PEARSONS).getData().equals(1.0));
            assertTrue("Unexpected difference in " + MetricConstants.MEAN_ABSOLUTE_ERROR,
                       testMe.get(MetricConstants.MEAN_ABSOLUTE_ERROR).getData().equals(5.0));
            assertTrue("Unexpected difference in " + MetricConstants.MEAN_ERROR,
                       testMe.get(MetricConstants.MEAN_ERROR).getData().equals(-5.0));
            assertTrue("Unexpected difference in " + MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                       testMe.get(MetricConstants.ROOT_MEAN_SQUARE_ERROR).getData().equals(5.0));

        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("Unexpected exception on processing project configuration '" + configPath + "'.");
        }
    }

}
