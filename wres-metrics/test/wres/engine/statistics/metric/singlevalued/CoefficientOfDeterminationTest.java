package wres.engine.statistics.metric.singlevalued;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.MetricInputException;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.metadata.MeasurementUnit;
import wres.datamodel.metadata.Metadata;
import wres.datamodel.metadata.MetricOutputMetadata;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.MetricParameterException;
import wres.engine.statistics.metric.MetricTestDataFactory;

/**
 * Tests the {@link CoefficientOfDetermination}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class CoefficientOfDeterminationTest
{
    
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link CoefficientOfDetermination}.
     */

    private CoefficientOfDetermination cod;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        this.cod = CoefficientOfDetermination.of();
    }

    /**
     * Compares the output from {@link CoefficientOfDetermination#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();
        MetricOutputMetadata m1 = MetricOutputMetadata.of( input.getRawData().size(),
                                                                   MeasurementUnit.of(),
                                                                   MeasurementUnit.of(),
                                                                   MetricConstants.COEFFICIENT_OF_DETERMINATION,
                                                                   MetricConstants.MAIN );

        //Compute normally
        DoubleScoreOutput actual = cod.apply( input );
        DoubleScoreOutput expected = DoubleScoreOutput.of( Math.pow( 0.9999999910148981, 2 ), m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }

    /**
     * Validates the output from {@link CoefficientOfDetermination#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        SingleValuedPairs input =
                SingleValuedPairs.of( Arrays.asList(), Metadata.of() );
 
        DoubleScoreOutput actual = cod.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link CoefficientOfDetermination#getName()} returns 
     * {@link MetricConstants#BIAS_FRACTION.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( cod.getName().equals( MetricConstants.COEFFICIENT_OF_DETERMINATION.toString() ) );
    }

    /**
     * Checks that the {@link CoefficientOfDetermination#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( cod.isDecomposable() );
    }

    /**
     * Checks that the {@link CoefficientOfDetermination#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( cod.isSkillScore() );
    }

    /**
     * Checks that the {@link CoefficientOfDetermination#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( cod.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Checks that the {@link CoefficientOfDetermination#getCollectionOf()} returns 
     * {@link MetricConstants#PEARSON_CORRELATION_COEFFICIENT}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( cod.getCollectionOf().equals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) );
    }    
    
    /**
     * Tests for an expected exception on calling {@link CoefficientOfDetermination#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'COEFFICIENT OF DETERMINATION'." );
        
        cod.apply( null );
    }    
    
    /**
     * Tests for an expected exception on calling {@link CoefficientOfDetermination#aggregate(DoubleScoreOutput)} with 
     * null input.
     */

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'COEFFICIENT OF DETERMINATION'." );
        
        cod.aggregate( null );
    }
    
}
