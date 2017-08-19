package wres.datamodel.metric;

import java.io.File;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.Assert;

import evs.io.xml.ProductFileIO;
import evs.metric.parameters.DoubleProcedureParameter;
import evs.metric.results.DoubleMatrix1DResult;
import evs.metric.results.MetricResult;
import evs.metric.results.MetricResultByLeadTime;
import evs.metric.results.MetricResultByThreshold;
import evs.metric.results.MetricResultKey;
import wres.datamodel.metric.MetricConstants.MetricDecompositionGroup;
import wres.datamodel.metric.SafeMetricOutputForProjectByLeadThreshold.SafeMetricOutputForProjectByLeadThresholdBuilder;
import wres.datamodel.metric.SafeMetricOutputMapByLeadThreshold.Builder;
import wres.datamodel.metric.Threshold.Operator;

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
     * testinput/wres/datamodel/metric/getScalarMetricOutputMapByLeadThresholdOne.xml.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByLeadThreshold<ScalarOutput> getScalarMetricOutputMapByLeadThresholdOne()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Builder<ScalarOutput> builder = new SafeMetricOutputMapByLeadThreshold.Builder<>();
        try
        {
            //Create the input file
            final File resultFile =
                                  new File("testinput/wres/datamodel/metric/getScalarMetricOutputMapByLeadThresholdOne.xml");
            final MetricResultByLeadTime data = ProductFileIO.read(resultFile);

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata(1000,
                                                                            metaFactory.getDimension(),
                                                                            metaFactory.getDimension("CMS"),
                                                                            MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
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
                    final QuantileThreshold q = outputFactory.getQuantileThreshold(constants[0],
                                                                                   probConstants[0],
                                                                                   Operator.GREATER);
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
            Assert.fail("Test failed : " + e.getMessage());
        }
        return builder.build();
    }

    /**
     * Returns a {@link MetricOutputMapByLeadThreshold} of {@link VectorOutput} comprising the CRPSS for selected
     * thresholds and forecast lead times. Reads the input data from
     * testinput/wres/datamodel/metric/getVectorMetricOutputMapByLeadThresholdOne.xml.
     * 
     * @return an output map of verification scores
     */

    public static MetricOutputMapByLeadThreshold<VectorOutput> getVectorMetricOutputMapByLeadThresholdOne()
    {
        final DataFactory outputFactory = DefaultDataFactory.getInstance();
        final MetadataFactory metaFactory = outputFactory.getMetadataFactory();
        final Builder<VectorOutput> builder = new SafeMetricOutputMapByLeadThreshold.Builder<>();
        try
        {
            //Create the input file
            final File resultFile =
                                  new File("testinput/wres/datamodel/metric/getVectorMetricOutputMapByLeadThresholdOne.xml");
            final MetricResultByLeadTime data = ProductFileIO.read(resultFile);

            final Iterator<MetricResultKey> d = data.getIterator();

            //Metric output metadata: add fake sample sizes as these are not readily available
            final MetricOutputMetadata meta = metaFactory.getOutputMetadata(1000,
                                                                            metaFactory.getDimension(),
                                                                            metaFactory.getDimension("CFS"),
                                                                            MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE,
                                                                            MetricConstants.CR_POT,
                                                                            metaFactory.getDatasetIdentifier("NPTP1",
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
                    final QuantileThreshold q = outputFactory.getQuantileThreshold(constants[0],
                                                                                   probConstants[0],
                                                                                   Operator.GREATER);
                    final MapBiKey<Integer, Threshold> key = outputFactory.getMapKey((int)leadTime, q);

                    //Build the scalar result
                    final MetricResult result = t.getResult(f);
                    final double[] res = ((DoubleMatrix1DResult)result).getResult().toArray();
                    final VectorOutput value = outputFactory.ofVectorOutput(res, MetricDecompositionGroup.CR_POT, meta);

                    //Append result
                    builder.put(key, value);
                }

            }

        }
        catch(final Exception e)
        {
            Assert.fail("Test failed : " + e.getMessage());
        }
        return builder.build();
    }

    /**
     * Returns a {@link MetricOutputForProjectByLeadThreshold} with fake data.
     * 
     * @return a {@link MetricOutputForProjectByLeadThreshold} with fake data
     */

    public static MetricOutputForProjectByLeadThreshold getMetricOutputForProjectByLeadThreshold()
    {
        //Prep
        SafeMetricOutputForProjectByLeadThresholdBuilder builder = new SafeMetricOutputForProjectByLeadThresholdBuilder();
        DataFactory factory = DefaultDataFactory.getInstance();
        MetadataFactory metaFactory = factory.getMetadataFactory();
        final MetricOutputMetadata fakeMeta =
                                            factory.getMetadataFactory()
                                                   .getOutputMetadata(1000,
                                                                      metaFactory.getDimension(),
                                                                      metaFactory.getDimension("CMS"),
                                                                      MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE);
        List<ScalarOutput> fakeData = new ArrayList<>();

        //Add some fake numbers
        fakeData.add(factory.ofScalarOutput(10.0, fakeMeta));
        fakeData.add(factory.ofScalarOutput(6.0, fakeMeta));
        fakeData.add(factory.ofScalarOutput(7.0, fakeMeta));
        fakeData.add(factory.ofScalarOutput(16.0, fakeMeta));

        //Build the input map
        MetricOutputMapByMetric<ScalarOutput> in = factory.ofMap(fakeData);

        //Fake lead time and threshold
        builder.addScalarOutput(factory.getMapKeyByLeadThreshold(1, 23.0, Operator.GREATER),
                                CompletableFuture.completedFuture(in));

        //Return data
        return builder.build();
    }

}
