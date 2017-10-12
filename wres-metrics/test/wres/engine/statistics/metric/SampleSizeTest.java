package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetadataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.MetricInputException;
import wres.datamodel.MetricOutputMetadata;
import wres.datamodel.ScalarOutput;
import wres.datamodel.SingleValuedPairs;
import wres.engine.statistics.metric.SampleSize.SampleSizeBuilder;

/**
 * Tests the {@link SampleSize}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class SampleSizeTest
{

    /**
     * Constructs a {@link SampleSize} and compares the actual result to the expected result. Also, checks the 
     * parameters of the metric.
     */

    @Test
    public void test1SampleSize()
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata(input.getData().size(),
                                                                  metaFac.getDimension(),
                                                                  metaFac.getDimension(),
                                                                  MetricConstants.SAMPLE_SIZE,
                                                                  MetricConstants.MAIN);
        //Build the metric
        final SampleSizeBuilder<SingleValuedPairs> b = new SampleSize.SampleSizeBuilder<>();
        b.setOutputFactory(outF);
        final SampleSize<SingleValuedPairs> ss = b.build();

        //Check the results
        final ScalarOutput actual = ss.apply(input);
        final ScalarOutput expected = outF.ofScalarOutput(input.size(), m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Sample Size.",
                   ss.getName().equals(metaFac.getMetricName(MetricConstants.SAMPLE_SIZE)));
        assertTrue("The Sample Size is not decomposable.", !ss.isDecomposable());
        assertTrue("The Sample Size is not a skill score.", !ss.isSkillScore());
        assertTrue("The Sample Size cannot be decomposed.", ss.getScoreOutputGroup() == ScoreOutputGroup.NONE);
    }
    
    /**
     * Constructs a {@link SampleSize} and checks for exceptional cases.
     */

    @Test
    public void test2Exceptions()
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();

        //Build the metric
        final SampleSizeBuilder<SingleValuedPairs> b = new SampleSize.SampleSizeBuilder<>();
        b.setOutputFactory(outF);
        final SampleSize<SingleValuedPairs> ss = b.build();

        //Check exceptions
        try
        {
            ss.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch(MetricInputException e)
        {          
        }
    }    
    
    

}
