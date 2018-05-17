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
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.singlevalued.SumOfSquareError.SumOfSquareErrorBuilder;

/**
 * Tests the {@link SumOfSquareError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SumOfSquareErrorTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Default instance of a {@link SumOfSquareError}.
     */

    private SumOfSquareError<SingleValuedPairs> sse;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        SumOfSquareErrorBuilder<SingleValuedPairs> b = new SumOfSquareError.SumOfSquareErrorBuilder<>();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        this.sse = b.build();
    }

    /**
     * Compares the output from {@link SumOfSquareError#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        //Generate some data
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsTwo();

        //Metadata for the output
        MetadataFactory metaFac = outF.getMetadataFactory();
        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getRawData().size(),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   metaFac.getDimension( "CMS" ),
                                                                   MetricConstants.SUM_OF_SQUARE_ERROR,
                                                                   MetricConstants.MAIN,
                                                                   metaFac.getDatasetIdentifier( "DRRC2",
                                                                                                 "SQIN",
                                                                                                 "HEFS",
                                                                                                 "ESP" ) );
        //Check the results
        DoubleScoreOutput actual = sse.apply( input );

        DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 4000039.29, m1 );

        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link SumOfSquareError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                outF.ofDiscreteProbabilityPairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );

        DoubleScoreOutput actual = sse.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link SumOfSquareError#getName()} returns 
     * {@link MetricConstants#SUM_OF_SQUARE_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( sse.getName().equals( MetricConstants.SUM_OF_SQUARE_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link SumOfSquareError#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( sse.isDecomposable() );
    }

    /**
     * Checks that the {@link SumOfSquareError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( sse.isSkillScore() );
    }

    /**
     * Checks that the {@link SumOfSquareError#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( sse.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Checks that the {@link SumOfSquareError#getCollectionOf()} returns 
     * {@link MetricConstants#SUM_OF_SQUARE_ERROR}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( sse.getCollectionOf().equals( MetricConstants.SUM_OF_SQUARE_ERROR ) );
    }

    /**
     * Tests for an expected exception on calling {@link SumOfSquareError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'SUM OF SQUARE ERROR'." );

        sse.apply( null );
    }

    /**
     * Tests for an expected exception on calling {@link SumOfSquareError#aggregate(DoubleScoreOutput)} with 
     * null input.
     */

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'SUM OF SQUARE ERROR'." );

        sse.aggregate( null );
    }

}
