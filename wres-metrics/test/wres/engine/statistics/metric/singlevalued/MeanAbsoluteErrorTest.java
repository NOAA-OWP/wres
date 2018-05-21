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
import wres.engine.statistics.metric.singlevalued.MeanAbsoluteError.MeanAbsoluteErrorBuilder;

/**
 * Tests the {@link MeanAbsoluteError}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class MeanAbsoluteErrorTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link MeanAbsoluteError}.
     */

    private MeanAbsoluteError mae;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        MeanAbsoluteErrorBuilder b = new MeanAbsoluteError.MeanAbsoluteErrorBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        this.mae = b.build();
    }

    /**
     * Compares the output from {@link MeanAbsoluteError#apply(SingleValuedPairs)} against expected output.
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
                                                                   MetricConstants.MEAN_ABSOLUTE_ERROR,
                                                                   MetricConstants.MAIN );
        //Check the results
        final DoubleScoreOutput actual = mae.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 201.37, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link MeanAbsoluteError#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                outF.ofDiscreteProbabilityPairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );
 
        DoubleScoreOutput actual = mae.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link MeanAbsoluteError#getName()} returns 
     * {@link MetricConstants#MEAN_ABSOLUTE_ERROR.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( mae.getName().equals( MetricConstants.MEAN_ABSOLUTE_ERROR.toString() ) );
    }

    /**
     * Checks that the {@link MeanAbsoluteError#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( mae.isDecomposable() );
    }

    /**
     * Checks that the {@link MeanAbsoluteError#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( mae.isSkillScore() );
    }

    /**
     * Checks that the {@link MeanAbsoluteError#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( mae.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Tests for an expected exception on calling {@link MeanAbsoluteError#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'MEAN ABSOLUTE ERROR'." );
        
        mae.apply( null );
    }    

}
