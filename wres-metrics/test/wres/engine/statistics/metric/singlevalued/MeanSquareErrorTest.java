package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.DiscreteProbabilityPairs;
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
 */
public final class MeanSquareErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link MeanSquareError}.
     */

    private MeanSquareError<SingleValuedPairs> mse;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        MeanSquareErrorBuilder<SingleValuedPairs> b = new MeanSquareError.MeanSquareErrorBuilder<>();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        this.mse = b.build();
    }

    /**
     * Compares the output from {@link MeanSquareError#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        //Metadata for the output
        MetadataFactory metaFac = outF.getMetadataFactory();
        MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getRawData().size(),
                                                             metaFac.getDimension(),
                                                             metaFac.getDimension(),
                                                             MetricConstants.MEAN_SQUARE_ERROR,
                                                             MetricConstants.MAIN );
        //Check the results
        final DoubleScoreOutput actual = mse.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 400003.929, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link MeanSquareError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                outF.ofDiscreteProbabilityPairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );

        DoubleScoreOutput actual = mse.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link MeanSquareError#getName()} returns 
     * {@link MetricConstants#MEAN_SQUARE_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( mse.getName().equals( MetricConstants.MEAN_SQUARE_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link MeanSquareError#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( mse.isDecomposable() );
    }

    /**
     * Checks that the {@link MeanSquareError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( mse.isSkillScore() );
    }

    /**
     * Checks that the {@link MeanSquareError#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( mse.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link MeanSquareError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN SQUARE ERROR'." );

        mse.apply( null );
    }

    /**
     * Tests for an expected exception on building a {@link MeanSquareError} with 
     * an unrecognized decomposition identifier.
     * @throws MetricParameterException if the metric could not be built for an unexpected reason
     */

    @Test
    public void testApplyExceptionOnUnsupportedDecomposition() throws MetricParameterException
    {
        exception.expect( MetricCalculationException.class );
        exception.expectMessage( "Decomposition is not currently implemented for the 'MEAN SQUARE ERROR'." );
        MeanSquareErrorBuilder<SingleValuedPairs> b = new MeanSquareErrorBuilder<>();
        b.setOutputFactory( outF );
        b.setDecompositionID( ScoreOutputGroup.CR );
        b.build().apply( MetricTestDataFactory.getSingleValuedPairsOne() );
    }

    /**
     * Tests for an expected exception on calling {@link MeanSquareError#aggregate(DoubleScoreOutput)} with 
     * null input.
     */

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN SQUARE ERROR'." );
        
        mse.aggregate( null );
    }  
    
}
