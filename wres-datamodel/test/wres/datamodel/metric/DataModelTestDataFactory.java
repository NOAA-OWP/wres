package wres.datamodel.metric;

import java.io.File;
import java.util.Iterator;

import org.junit.Assert;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import wres.datamodel.metric.SafeMetricOutputMapByLeadThreshold.Builder;
import wres.datamodel.metric.Threshold.Condition;

/**
 * Factory class for generating test datasets for metric calculations.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class DataModelTestDataFactory
{

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link ScalarOutput} comprising the CRPSS for selected
     * thresholds and forecast lead times. Reads the input data from
     * testinput/wres/datamodel/metric/getMetricOutputMapByLeadThresholdOne.xml.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByLeadThreshold<ScalarOutput> getMetricOutputMapByLeadThresholdOne()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Builder<ScalarOutput> builder = new SafeMetricOutputMapByLeadThreshold.Builder<>();
        try
        {
            //Create the input file
            final File resultFile =
                                  new File("testinput/wres/datamodel/metric/getMetricOutputMapByLeadThresholdOne.xml");
            final MetricResultByLeadTime data = ProductFileIO.read(resultFile);

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata(1000,
                                                                            metaFactory.getDimension(),
                                                                            metaFactory.getDimension("CMS"),
                                                                            MetricConstants.MEAN_CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                            MetricConstants.MAIN,
                                                                            metaFactory.getDatasetIdentifier("DRRC2",
                                                                                                             "SQIN",
                                                                                                             "HEFS",
                                                                                                             "ESP"));

            //Iterate through the lead times
            while(d.hasNext())
            {
                //Set the lead time
                final double leadTime = (Double)d.next().getKey();
                final MetricResultByThreshold t = (MetricResultByThreshold)data.getResult(leadTime);
                final Iterator<MetricResultKey> e = t.getIterator();
                //Iterate through the thresholds
                while(e.hasNext())
                {
                    //Build the quantile
                    final DoubleProcedureParameter f = (DoubleProcedureParameter)e.next().getKey();
                    final double[] constants = f.getParValReal().getConstants();
                    final double[] probConstants = f.getParVal().getConstants();
                    final Quantile q = outputFactory.getQuantile(constants[0], probConstants[0], Condition.GREATER);
                    final MapBiKey<Integer, Threshold> key = outputFactory.getMapKey((int)leadTime, q);

                    //Build the scalar result
                    final MetricResult result = t.getResult(f);
                    final double[] res = ((DoubleMatrix1DResult)result).getResult().toArray();
                    final ScalarOutput value = outputFactory.ofScalarOutput(res[0], meta);

                    //Append result
                    builder.put(key, value);
                }

            }

        }
        catch(final Exception e)
        {
            e.printStackTrace();
            Assert.fail("Test failed : " + e.getMessage());
        }
        return builder.build();
    }
}
