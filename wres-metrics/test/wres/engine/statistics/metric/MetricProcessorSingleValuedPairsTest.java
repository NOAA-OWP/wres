package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.file.Paths;

import org.junit.Test;

import wres.config.generated.ProjectConfig;
import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.Metadata;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricOutputGroup;
import wres.datamodel.MetricOutputForProjectByLeadThreshold;
import wres.datamodel.MetricOutputMapByLeadThreshold;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.io.config.ProjectConfigPlus;

/**
 * Tests the {@link MetricProcessorSingleValuedPairsByLeadTime}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MetricProcessorSingleValuedPairsTest
{

    private final DataFactory dataFactory = DefaultDataFactory.getInstance();

    /**
     * Tests the construction of a {@link MetricProcessorSingleValuedPairsByLeadTime} and application of
     * {@link MetricProcessorSingleValuedPairsByLeadTime#apply(wres.datamodel.SingleValuedPairs)} to configuration obtained
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
            MetricProcessor<MetricOutputForProjectByLeadThreshold> processor = MetricFactory.getInstance(dataFactory)
                                                                                            .getMetricProcessorByLeadTime(config);
            SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
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
                       bias.getValue(0).getData().equals(1.6666666666666667));
            assertTrue("Unexpected difference in " + MetricConstants.COEFFICIENT_OF_DETERMINATION,
                       cod.getValue(0).getData().equals(1.0));
            assertTrue("Unexpected difference in " + MetricConstants.CORRELATION_PEARSONS,
                       rho.getValue(0).getData().equals(1.0));
            assertTrue("Unexpected difference in " + MetricConstants.MEAN_ABSOLUTE_ERROR,
                       mae.getValue(0).getData().equals(5.0));
            assertTrue("Unexpected difference in " + MetricConstants.MEAN_ERROR, me.getValue(0).getData().equals(5.0));
            assertTrue("Unexpected difference in " + MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                       rmse.getValue(0).getData().equals(5.0));

        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("Unexpected exception on processing project configuration '" + configPath + "'.");
        }
    }

    /**
     * Tests the construction of a {@link MetricProcessorSingleValuedPairsByLeadTime} and application of
     * {@link MetricProcessorSingleValuedPairsByLeadTime#apply(wres.datamodel.SingleValuedPairs)} to configuration obtained
     * from testinput/metricProcessorSingleValuedPairsTest/test1ApplyNoThresholds.xml and pairs obtained from
     * {@link MetricTestDataFactory#getSingleValuedPairsFour()}. Tests the output for multiple calls with separate
     * forecast lead times.
     */

    @Test
    public void test2ApplyNoThresholds()
    {
        final DataFactory metIn = DefaultDataFactory.getInstance();
        String configPath = "testinput/metricProcessorSingleValuedPairsTest/test1ApplyNoThresholds.xml";
        try
        {
            ProjectConfig config = ProjectConfigPlus.from(Paths.get(configPath)).getProjectConfig();
            MetricProcessor<MetricOutputForProjectByLeadThreshold> processor =
                                                                             MetricFactory.getInstance(metIn)
                                                                                          .getMetricProcessorByLeadTime(config,
                                                                                                              MetricOutputGroup.SCALAR);
            SingleValuedPairs pairs = MetricTestDataFactory.getSingleValuedPairsFour();
            final MetadataFactory metFac = metIn.getMetadataFactory();
            //Generate results for 10 nominal lead times
            for(int i = 1; i < 11; i++)
            {
                final Metadata meta = metFac.getMetadata(metFac.getDimension("CMS"),
                                                         metFac.getDatasetIdentifier("DRRC2", "SQIN", "HEFS"),
                                                         i);
                SingleValuedPairs next = metIn.ofSingleValuedPairs(pairs.getData(), meta);
                processor.apply(next);
            }
            processor.getStoredMetricOutput().getScalarOutput().forEach((key, value) -> {
                assertTrue("Expected results at ten forecast lead times for the " + key.getKey(),
                           value.size() == 10);
            });
        }
        catch(Exception e)
        {
            e.printStackTrace();
            fail("Unexpected exception on processing project configuration '" + configPath + "'.");
        }
    }

}
