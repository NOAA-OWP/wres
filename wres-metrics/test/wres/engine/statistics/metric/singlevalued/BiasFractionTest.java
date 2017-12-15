package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.BiasFraction;
import wres.engine.statistics.metric.singlevalued.BiasFraction.BiasFractionBuilder;

/**
 * Tests the {@link BiasFraction}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class BiasFractionTest
{

    /**
     * Constructs a {@link BiasFraction} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1BiasFraction() throws MetricParameterException
    {
        //Obtain the factories
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MetadataFactory metaFac = outF.getMetadataFactory();

        //Generate some data
        final SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension(),
                                                                   MetricConstants.BIAS_FRACTION,
                                                                   MetricConstants.MAIN );
        //Build the metric
        final BiasFractionBuilder b = new BiasFraction.BiasFractionBuilder();
        b.setOutputFactory( outF );
        final BiasFraction bf = b.build();

        //Check the results
        final ScalarOutput actual = bf.apply( input );
        final ScalarOutput expected = outF.ofScalarOutput( 0.056796298, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for the Bias Fraction.",
                    bf.getName().equals( MetricConstants.BIAS_FRACTION.toString() ) );
        assertTrue( "The Bias Fraction is not decomposable.", !bf.isDecomposable() );
        assertTrue( "The Bias Fraction is not a skill score.", !bf.isSkillScore() );
        assertTrue( "The Bias Fraction cannot be decomposed.", bf.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Constructs a {@link BiasFraction} and checks for exceptional cases.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final BiasFractionBuilder b = new BiasFraction.BiasFractionBuilder();
        b.setOutputFactory( outF );
        final BiasFraction bf = b.build();

        //Check the exceptions
        try
        {
            bf.apply( (SingleValuedPairs) null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }

}
