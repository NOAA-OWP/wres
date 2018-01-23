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
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricCalculationException;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.MeanSquareError.MeanSquareErrorBuilder;

/**
 * Tests the {@link MeanSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.1
 */
public final class MeanSquareErrorTest
{

    /**
     * Constructs a {@link MeanSquareError} and compares the actual result to the expected result. Also, checks the
     * parameters of the metric.
     * @throws MetricParameterException if the metric construction fails 
     */

    @Test
    public void test1MeanSquareError() throws MetricParameterException
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
                                                                   MetricConstants.MEAN_SQUARE_ERROR,
                                                                   MetricConstants.NONE );

        //Build the metric
        final MeanSquareErrorBuilder<SingleValuedPairs> b = new MeanSquareError.MeanSquareErrorBuilder<>();
        b.setOutputFactory( outF );
        final MeanSquareError<SingleValuedPairs> mse = b.build();

        //Check the results
        final DoubleScoreOutput actual = mse.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 400003.929, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );

        //Check the parameters
        assertTrue( "Unexpected name for the Mean Square Error.",
                    mse.getName().equals( MetricConstants.MEAN_SQUARE_ERROR.toString() ) );
        assertTrue( "The Mean Square Error is decomposable.", mse.isDecomposable() );
        assertTrue( "The Mean Square Error is not a skill score.", !mse.isSkillScore() );
        assertTrue( "Expected no decomposition for the Mean Square Error.",
                    mse.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Constructs a {@link MeanSquareError} and checks for exceptional cases.
     * @throws MetricParameterException if the metric construction fails
     */

    @Test
    public void test2Exceptions() throws MetricParameterException
    {
        //Build the metric
        final DataFactory outF = DefaultDataFactory.getInstance();
        final MeanSquareErrorBuilder<SingleValuedPairs> b =
                new MeanSquareError.MeanSquareErrorBuilder<>();
        b.setOutputFactory( outF );
        final MeanSquareError<SingleValuedPairs> mse = b.build();

        //Check the exceptions
        try
        {
            ((MeanSquareErrorBuilder<SingleValuedPairs>)b.setDecompositionID( null )).build();
            fail( "Expected an invalid decomposition identifier." );
        }
        catch ( final Exception e )
        {
        }
        try
        {
            ((MeanSquareErrorBuilder<SingleValuedPairs>)b.setDecompositionID( ScoreOutputGroup.CR_AND_LBR ))
             .build()
             .apply( MetricTestDataFactory.getSingleValuedPairsOne() );
            fail( "Expected an exception, indicating that decomposition has not been implemented." );
        }
        catch ( final MetricCalculationException e )
        {
        }
        try
        {
            mse.apply( null );
            fail( "Expected an exception on null input." );
        }
        catch ( MetricInputException e )
        {
        }
    }
}
