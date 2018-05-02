package wres.engine.statistics.metric.discreteprobability;

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
import wres.datamodel.metadata.MetadataFactory;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;
import wres.engine.statistics.metric.discreteprobability.BrierScore.BrierScoreBuilder;

/**
 * Tests the {@link BrierScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class BrierScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link BrierScore}.
     */

    private BrierScore brierScore;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        BrierScoreBuilder b = new BrierScore.BrierScoreBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        b.setDecompositionID( ScoreOutputGroup.NONE );
        this.brierScore = b.build();
    }

    /**
     * Compares the output from {@link BrierScore#apply(DiscreteProbabilityPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        // Generate some data
        DiscreteProbabilityPairs input = MetricTestDataFactory.getDiscreteProbabilityPairsOne();
        MetadataFactory metaFac = outF.getMetadataFactory();

        // Metadata for the output
        MetricOutputMetadata m1 =
                metaFac.getOutputMetadata( input.getRawData().size(),
                                           metaFac.getDimension(),
                                           metaFac.getDimension(),
                                           MetricConstants.BRIER_SCORE,
                                           MetricConstants.MAIN,
                                           metaFac.getDatasetIdentifier( "DRRC2", "SQIN", "HEFS" ) );

        // Check the results       
        DoubleScoreOutput actual = brierScore.apply( input );
        DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 0.26, m1 );
        assertTrue( "Actual: " + actual.getData()
                    + ". Expected: "
                    + expected.getData()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link BrierScore#apply(DiscreteProbabilityPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                outF.ofDiscreteProbabilityPairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );
 
        DoubleScoreOutput actual = brierScore.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link BrierScore#getName()} returns {@link MetricConstants.BRIER_SCORE.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( brierScore.getName().equals( MetricConstants.BRIER_SCORE.toString() ) );
    }

    /**
     * Checks that the {@link BrierScore#isDecomposable()} returns <code>true</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertTrue( brierScore.isDecomposable() );
    }

    /**
     * Checks that the {@link BrierScore#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( brierScore.isSkillScore() );
    }

    /**
     * Checks that the {@link BrierScore#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( brierScore.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Checks that the {@link BrierScore#isProper()} returns <code>true</code>.
     */

    @Test
    public void testIsProper()
    {
        assertTrue( brierScore.isProper() );
    }

    /**
     * Checks that the {@link BrierScore#isStrictlyProper()} returns <code>true</code>.
     */

    @Test
    public void testIsStrictlyProper()
    {
        assertTrue( brierScore.isStrictlyProper() );
    }
    
    /**
     * Tests for an expected exception on calling {@link BrierScore#apply(DiscreteProbabilityPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'BRIER SCORE'." );
        
        brierScore.apply( null );
    }

}
