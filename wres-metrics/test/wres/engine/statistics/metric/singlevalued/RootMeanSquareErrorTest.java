package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.outputs.MetricOutputMetadata;
import wres.datamodel.outputs.ScalarOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareError;
import wres.engine.statistics.metric.singlevalued.RootMeanSquareError.RootMeanSquareErrorBuilder;

/**
 * Tests the {@link RootMeanSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class RootMeanSquareErrorTest
{

    /**
     * Constructs a {@link RootMeanSquareError} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void test1RootMeanSquareError() throws MetricParameterException
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
                                                                   MetricConstants.ROOT_MEAN_SQUARE_ERROR,
                                                                   MetricConstants.MAIN );

        //Build the metric
        final RootMeanSquareErrorBuilder b = new RootMeanSquareError.RootMeanSquareErrorBuilder();
        b.setOutputFactory( outF );
        final RootMeanSquareError mse = b.build();

        //Check the results
        final ScalarOutput actual = mse.apply( input );
        final ScalarOutput expected = outF.ofScalarOutput( 632.4586381732801, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for the Root Mean Square Error.",
                    mse.getName().equals( MetricConstants.ROOT_MEAN_SQUARE_ERROR.toString() ) );
        assertTrue( "The Root Mean Square Error is not decomposable.", !mse.isDecomposable() );
        assertTrue( "The Root Mean Square Error is not a skill score.", !mse.isSkillScore() );
        assertTrue( "Expected no decomposition for the Root Mean Square Error.",
                    mse.getScoreOutputGroup() == ScoreOutputGroup.NONE );

    }

}
