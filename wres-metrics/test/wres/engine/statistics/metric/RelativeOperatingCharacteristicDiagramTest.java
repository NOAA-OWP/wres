package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.MetricDimension;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.MultiVectorOutput;
import wres.engine.statistics.metric.RelativeOperatingCharacteristicDiagram.RelativeOperatingCharacteristicBuilder;

/**
 * Tests the {@link RelativeOperatingCharacteristicDiagram}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class RelativeOperatingCharacteristicDiagramTest
{

    /**
     * Constructs a {@link RelativeOperatingCharacteristicDiagram} and compares the actual result to the expected result. Also,
     * checks the parameters of the metric. Uses the data from
     * {@link MetricTestDataFactory#getDiscreteProbabilityPairsThree()}.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1RelativeOperatingCharacteristic() throws MetricParameterException
    {
        //Generate some data
        final DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsThree();

        //Build the metric
        final RelativeOperatingCharacteristicBuilder b = new RelativeOperatingCharacteristicBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();
        b.setOutputFactory(outF);

        final RelativeOperatingCharacteristicDiagram roc = b.build();

        //Metadata for the output
        final MetricOutputMetadata m1 =
                                      metaFac.getOutputMetadata(input.size(),
                                                                metaFac.getDimension(),
                                                                metaFac.getDimension(),
                                                                MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM,
                                                                MetricConstants.MAIN,
                                                                metaFac.getDatasetIdentifier("Tampere", "MAP", "FMI"));

        //Check the results       
        final MultiVectorOutput actual = roc.apply(input);
        double[] expectedPOD = new double[]{0.0, 0.13580246913580246, 0.2345679012345679, 0.43209876543209874,
            0.6296296296296297, 0.7037037037037037, 0.8024691358024691, 0.8518518518518519, 0.9135802469135802,
            0.9753086419753086, 1.0};
        double[] expectedPOFD = new double[]{0.0, 0.007518796992481203, 0.018796992481203006, 0.04887218045112782,
            0.11654135338345864, 0.17669172932330826, 0.22932330827067668, 0.2857142857142857, 0.42105263157894735,
            0.6240601503759399, 1.0};
        Map<MetricDimension, double[]> output = new HashMap<>();
        output.put(MetricDimension.PROBABILITY_OF_DETECTION, expectedPOD);
        output.put(MetricDimension.PROBABILITY_OF_FALSE_DETECTION, expectedPOFD);
        final MultiVectorOutput expected = outF.ofMultiVectorOutput(output, m1);
        assertTrue("Difference between actual and expected ROC.", actual.equals(expected));
        //Check the parameters
        assertTrue("Unexpected name for the Relative Operating Characteristic.",
                   roc.getName().equals(metaFac.getMetricName(MetricConstants.RELATIVE_OPERATING_CHARACTERISTIC_DIAGRAM)));
    }

    /**
     * Constructs a {@link RelativeOperatingCharacteristicDiagram} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test4Exceptions() throws MetricParameterException
    {
        
        //Build the metric
        final RelativeOperatingCharacteristicBuilder b = new RelativeOperatingCharacteristicBuilder();
        final DataFactory outF = DefaultDataFactory.getInstance();
        b.setOutputFactory(outF);

        final RelativeOperatingCharacteristicDiagram roc = b.build();

        //Check exceptions
        try
        {
            roc.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch(MetricInputException e)
        {          
        }
    }       
    
    
}
