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
import wres.engine.statistics.metric.MeanError.MeanErrorBuilder;

/**
 * Tests the {@link MeanError}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanErrorTest
{

    /**
     * Constructs a {@link MeanError} and compares the actual result to the expected result. Also, checks the parameters
     * of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1MeanError() throws MetricParameterException
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
                                                                  MetricConstants.MEAN_ERROR,
                                                                  MetricConstants.MAIN);
        //Build the metric
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        b.setOutputFactory(outF);
        final MeanError me = b.build();

        //Check the results
        final ScalarOutput actual = me.apply(input);
        final ScalarOutput expected = outF.ofScalarOutput(200.55, m1);
        assertTrue("Actual: " + actual.getData().doubleValue() + ". Expected: " + expected.getData().doubleValue()
            + ".", actual.equals(expected));

        //Check the parameters
        assertTrue("Unexpected name for the Mean Error.",
                   me.getName().equals(metaFac.getMetricName(MetricConstants.MEAN_ERROR)));
        assertTrue("The Mean Error is not decomposable.", !me.isDecomposable());
        assertTrue("The Mean Error is not a skill score.", !me.isSkillScore());
        assertTrue("The Mean Error cannot be decomposed.", me.getScoreOutputGroup() == ScoreOutputGroup.NONE);
    }
    
    /**
     * Constructs a {@link MeanError} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed 
     */

    @Test
    public void test4Exceptions() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();

        //Build the metric
        final MeanErrorBuilder b = new MeanError.MeanErrorBuilder();
        b.setOutputFactory(outF);
        final MeanError me = b.build();

        //Check exceptions
        try
        {
            me.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch(MetricInputException e)
        {          
        }
    }     

}
