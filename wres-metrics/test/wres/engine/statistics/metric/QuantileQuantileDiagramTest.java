package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.util.Precision;
import org.junit.Test;

import wres.datamodel.PairOfDoubles;
import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MultiVectorOutput;
import wres.datamodel.metric.SingleValuedPairs;
import wres.engine.statistics.metric.QuantileQuantileDiagram.QuantileQuantileDiagramBuilder;

/**
 * Tests the {@link QuantileQuantileDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class QuantileQuantileDiagramTest
{

    /**
     * Constructs a {@link QuantileQuantileDiagram} and compares the actual result to the expected result. Also, checks
     * the parameters of the metric.
     */

    @Test
    public void test1QuantileQuantileDiagram()
    {
        //Build the metric
        final QuantileQuantileDiagramBuilder b = new QuantileQuantileDiagramBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory(outF);

        final QuantileQuantileDiagram qq = b.build();

        //Generate some data
        final List<PairOfDoubles> values = new ArrayList<>();
        for(int i = 1; i < 1001; i++)
        {
            double left = (i * 1001.0) / 1000.0;
            double right = left;
            values.add(outF.pairOf(left, right));
        }

        final SingleValuedPairs input = outF.ofSingleValuedPairs(values, metaFac.getMetadata(101));

        //Metadata for the output
        final MetricOutputMetadata m1 =
                                      metaFac.getOutputMetadata(input.getMetadata().getSampleSize(),
                                                                metaFac.getDimension(),
                                                                metaFac.getDimension(),
                                                                MetricConstants.QUANTILE_QUANTILE_DIAGRAM,
                                                                MetricConstants.MAIN,
                                                                metaFac.getDatasetIdentifier("Tampere", "MAP", "FMI"));

        //Check the results       
        final MultiVectorOutput actual = qq.apply(input);
        double[] actualObs = actual.get(MetricConstants.OBSERVED_QUANTILES).getDoubles();
        double[] actualPred = actual.get(MetricConstants.PREDICTED_QUANTILES).getDoubles();

        //Check the first pair of quantiles, which should map to the first entry, since the lower bound is unknown
        assertTrue("Difference between actual and expected quantiles of observations [" + 1.001 + ", "
            + actualObs[0] + "].", Double.compare(actualObs[0], 1.001) == 0);
        assertTrue("Difference between actual and expected quantiles of predictions [" + 1.001 + ", "
            + actualPred[0] + "].", Double.compare(actualPred[0], 1.001) == 0);

        //Expected values
        for(int i = 1; i < 1000; i++)
        {
            double expectedObserved = i + 1;
            double expectedPredicted = i + 1;
            double actualObserved = Precision.round(actualObs[i], 5);
            double actualPredicted = Precision.round(actualPred[i], 5);
            assertTrue("Difference between actual and expected quantiles of observations [" + expectedObserved + ", "
                + actualObserved + "].", Double.compare(actualObserved, expectedObserved) == 0);
            assertTrue("Difference between actual and expected quantiles of predictions [" + expectedPredicted + ", "
                + actualPredicted + "].", Double.compare(actualPredicted, expectedPredicted) == 0);
        }

        //Check the parameters
        assertTrue("Unexpected name for the Quantile-Quantile Diagram.",
                   qq.getName().equals(metaFac.getMetricName(MetricConstants.QUANTILE_QUANTILE_DIAGRAM)));

    }

}
