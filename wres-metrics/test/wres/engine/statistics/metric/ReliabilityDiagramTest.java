package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import wres.datamodel.metric.DataFactory;
import wres.datamodel.metric.DefaultDataFactory;
import wres.datamodel.metric.DiscreteProbabilityPairs;
import wres.datamodel.metric.MetadataFactory;
import wres.datamodel.metric.MetricConstants;
import wres.datamodel.metric.MetricOutputMetadata;
import wres.datamodel.metric.MultiVectorOutput;
import wres.engine.statistics.metric.ReliabilityDiagram.ReliabilityDiagramBuilder;

/**
 * Tests the {@link ReliabilityDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class ReliabilityDiagramTest
{

    /**
     * Constructs a {@link ReliabilityDiagram} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric. Uses the data from {@link MetricTestDataFactory#getDiscreteProbabilityPairsThree()}.
     */

    @Test
    public void test1ReliabilityDiagram()
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsThree();

        //Build the metric
        final ReliabilityDiagramBuilder b = new ReliabilityDiagram.ReliabilityDiagramBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory(outF);

        final ReliabilityDiagram rel = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                                      metaFac.getOutputMetadata(input.size(),
                                                                metaFac.getDimension(),
                                                                metaFac.getDimension(),
                                                                MetricConstants.RELIABILITY_DIAGRAM,
                                                                MetricConstants.MAIN,
                                                                metaFac.getDatasetIdentifier("Tampere", "MAP", "FMI"));

        //Check the results       
        final MultiVectorOutput actual = rel.apply(input);
        double[] expectedFProb = new double[]{0.05490196078431369, 0.19999999999999984, 0.3000000000000002,
            0.40000000000000013, 0.5, 0.5999999999999998, 0.6999999999999996, 0.8000000000000003, 0.9000000000000002,
            1.0};
        double[] expectedOProb = new double[]{0.0196078431372549, 0.0847457627118644, 0.12195121951219512,
            0.21052631578947367, 0.36363636363636365, 0.2727272727272727, 0.47058823529411764, 0.6666666666666666,
            0.7272727272727273, 0.8461538461538461};
        double[] expectedSample = new double[]{102.0, 59.0, 41.0, 19.0, 22.0, 22.0, 34.0, 24.0, 11.0, 13.0};

        Map<MetricConstants, double[]> output = new HashMap<>();
        output.put(MetricConstants.FORECAST_PROBABILITY, expectedFProb);
        output.put(MetricConstants.OBSERVED_GIVEN_FORECAST_PROBABILITY, expectedOProb);
        output.put(MetricConstants.SAMPLE_SIZE, expectedSample);

        final MultiVectorOutput expected = outF.ofMultiVectorOutput(output, m1);
        assertTrue("Difference between actual and expected Reliability Diagram.", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Reliability Diagram.",
                   rel.getName().equals(metaFac.getMetricName(MetricConstants.RELIABILITY_DIAGRAM)));

    }

}
