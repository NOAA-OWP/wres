package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.EnsemblePairs;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.metric.MetricOutputMapByLeadThreshold;
import wres.datamodel.metric.ScalarOutput;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessorEnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorEnsemblePairsTest
{

    private final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * Tests the construction of a {@link MetricProcessorEnsemblePairs} and application of
     * {@link MetricProcessorEnsemblePairs#apply(wres.datamodel.metric.EnsemblePairs)} to configuration obtained from
     * testinput/metricProcessorEnsemblePairsTest/test1ApplyNoThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getEnsemblePairsOne()}.
     */

    @Test
    public void test1ApplyNoThresholds()
    {
        String configPath = "testinput/metricProcessorEnsemblePairsTest/test1ApplyNoThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from(Paths.get(configPath)).getProjectConfig();
            MetricProcessorEnsemblePairs processor = (MetricProcessorEnsemblePairs)MetricProcessor.of(dataFactory,
                                                                                                      config);
            EnsemblePairs pairs = MetricTestDataFactory.getEnsemblePairsOne();
            MetricOutputForProjectByLeadThreshold results = processor.apply(pairs);
            MetricOutputMapByLeadThreshold<ScalarOutput> bias = results.getScalarOutput()
                                                                       .get(MetricConstants.BIAS_FRACTION);
            MetricOutputMapByLeadThreshold<ScalarOutput> cod =
                                                             results.getScalarOutput()
                                                                    .get(MetricConstants.COEFFICIENT_OF_DETERMINATION);
            MetricOutputMapByLeadThreshold<ScalarOutput> rho = results.getScalarOutput()
                                                                      .get(MetricConstants.CORRELATION_PEARSONS);
            MetricOutputMapByLeadThreshold<ScalarOutput> mae = results.getScalarOutput()
                                                                      .get(MetricConstants.MEAN_ABSOLUTE_ERROR);
            MetricOutputMapByLeadThreshold<ScalarOutput> me = results.getScalarOutput().get(MetricConstants.MEAN_ERROR);
            MetricOutputMapByLeadThreshold<ScalarOutput> rmse = results.getScalarOutput()
                                                                       .get(MetricConstants.ROOT_MEAN_SQUARE_ERROR);

            //Test contents
            assertTrue("Unexpected difference in " + MetricConstants.BIAS_FRACTION,
                       bias.getValue(0).getData().equals(0.032093836077598345));
            assertTrue("Unexpected difference in " + MetricConstants.COEFFICIENT_OF_DETERMINATION,
                       cod.getValue(0).getData().equals(0.7873367083297588));
            assertTrue("Unexpected difference in " + MetricConstants.CORRELATION_PEARSONS,
                       rho.getValue(0).getData().equals(0.8873199582618204));
            assertTrue("Unexpected difference in " + MetricConstants.MEAN_ABSOLUTE_ERROR,
                       mae.getValue(0).getData().equals(11.009512537315405));
            assertTrue("Unexpected difference in " + MetricConstants.MEAN_ERROR,
                       me.getValue(0).getData().equals(1.157869354367079));
            assertTrue("Unexpected difference in " + MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                       rmse.getValue(0).getData().equals(41.01563032408479));
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("Unexpected exception on processing project configuration '" + configPath + "'.");
        }
    }

}
