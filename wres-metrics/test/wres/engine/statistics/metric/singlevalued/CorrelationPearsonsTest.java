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
import wres.engine.statistics.metric.singlevalued.CorrelationPearsons.CorrelationPearsonsBuilder;

/**
 * Tests the {@link CorrelationPearsons}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class CorrelationPearsonsTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Default instance of a {@link CorrelationPearsons}.
     */

    private CorrelationPearsons rho;

    /**
     * Instance of a data factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest() throws MetricParameterException
    {
        CorrelationPearsonsBuilder b = new CorrelationPearsons.CorrelationPearsonsBuilder();
        this.outF = DefaultDataFactory.getInstance();
        b.setOutputFactory( outF );
        this.rho = b.build();
    }

    /**
     * Compares the output from {@link CorrelationPearsons#apply(SingleValuedPairs)} against expected output.
     */

    @Test
    public void testApply()
    {
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();
        
        MetadataFactory metaFac = outF.getMetadataFactory();

        final MetricOutputMetadata m1 = metaFac.getOutputMetadata( input.getRawData().size(),
                                                                   metaFac.getDimension(),
                                                                   metaFac.getDimension(),
                                                                   MetricConstants.PEARSON_CORRELATION_COEFFICIENT,
                                                                   MetricConstants.MAIN );

        //Compute normally
        final DoubleScoreOutput actual = rho.apply( input );
        final DoubleScoreOutput expected = outF.ofDoubleScoreOutput( 0.9999999910148981, m1 );
        assertTrue( "Actual: " + actual.getData().doubleValue()
                    + ". Expected: "
                    + expected.getData().doubleValue()
                    + ".",
                    actual.equals( expected ) );
    }
    
    /**
     * Compares the output from {@link CorrelationPearsons#aggregate(DoubleScoreOutput)} against expected output.
     */

    @Test
    public void testAggregate()
    {
        SingleValuedPairs input = MetricTestDataFactory.getSingleValuedPairsOne();

        assertTrue( rho.apply( input ).equals( rho.aggregate( rho.getInputForAggregation( input ) ) ) );
    }    

    /**
     * Validates the output from {@link CorrelationPearsons#apply(SingleValuedPairs)} when supplied with no data.
     */

    @Test
    public void testApplyWithNoData()
    {
        // Generate empty data
        DiscreteProbabilityPairs input =
                outF.ofDiscreteProbabilityPairs( Arrays.asList(), outF.getMetadataFactory().getMetadata() );
 
        DoubleScoreOutput actual = rho.apply( input );

        assertTrue( actual.getData().isNaN() );
    }

    /**
     * Checks that the {@link CorrelationPearsons#getName()} returns 
     * {@link MetricConstants#PEARSON_CORRELATION_COEFFICIENT.toString()}
     */

    @Test
    public void testGetName()
    {
        assertTrue( rho.getName().equals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT.toString() ) );
    }

    /**
     * Checks that the {@link CorrelationPearsons#isDecomposable()} returns <code>false</code>.
     */

    @Test
    public void testIsDecomposable()
    {
        assertFalse( rho.isDecomposable() );
    }

    /**
     * Checks that the {@link CorrelationPearsons#isSkillScore()} returns <code>false</code>.
     */

    @Test
    public void testIsSkillScore()
    {
        assertFalse( rho.isSkillScore() );
    }
    
    /**
     * Checks that the {@link CorrelationPearsons#hasRealUnits()} returns <code>false</code>.
     */

    @Test
    public void testhasRealUnits()
    {
        assertFalse( rho.hasRealUnits() );
    }   

    /**
     * Checks that the {@link CorrelationPearsons#getScoreOutputGroup()} returns the result provided on construction.
     */

    @Test
    public void testGetScoreOutputGroup()
    {
        assertTrue( rho.getScoreOutputGroup() == ScoreOutputGroup.NONE );
    }

    /**
     * Checks that the {@link CorrelationPearsons#getCollectionOf()} returns 
     * {@link MetricConstants#PEARSON_CORRELATION_COEFFICIENT}.
     */

    @Test
    public void testGetCollectionOf()
    {
        assertTrue( rho.getCollectionOf().equals( MetricConstants.PEARSON_CORRELATION_COEFFICIENT ) );
    }    
    
    /**
     * Tests for an expected exception on calling {@link CorrelationPearsons#apply(SingleValuedPairs)} with null 
     * input.
     */

    @Test
    public void testApplyExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'PEARSON CORRELATION COEFFICIENT'." );
        
        rho.apply( null );
    }    
    
    /**
     * Tests for an expected exception on calling {@link CorrelationPearsons#aggregate(DoubleScoreOutput)} with 
     * null input.
     */

    @Test
    public void testAggregateExceptionOnNullInput()
    {
        exception.expect( MetricInputException.class );
        exception.expectMessage( "Specify non-null input to the 'PEARSON CORRELATION COEFFICIENT'." );
        
        rho.aggregate( null );
    }    

}
