package wres.engine.statistics.metric;

import static org.junit.Assert.assertTrue;

import java.util.Objects;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.DataFactory;
import wres.datamodel.DefaultDataFactory;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.ScoreOutputGroup;
import wres.datamodel.inputs.pairs.SingleValuedPairs;
import wres.datamodel.outputs.DoubleScoreOutput;
import wres.engine.statistics.metric.SampleSize.SampleSizeBuilder;

/**
 * Tests the {@link OrdinaryScore}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class OrdinaryScoreTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    /**
     * Output factory.
     */

    private DataFactory outF;

    @Before
    public void setupBeforeEachTest()
    {
        outF = DefaultDataFactory.getInstance();
    }       
    
    /**
     * Constructs a {@link OrdinaryScore} and compares the actual result to the expected result.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testToString() throws MetricParameterException
    {
        SampleSizeBuilder<SingleValuedPairs> b = new SampleSizeBuilder<>();
        b.setOutputFactory( outF );
        SampleSize<SingleValuedPairs> ss = b.build();

        assertTrue( "SAMPLE SIZE".equals( ss.toString() ) );
    }     
    
    /**
     * Checks that {@link OrdinaryScore#getDataFactory()} returns a non-null data factory.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testGetDataFactory() throws MetricParameterException
    {
        SampleSizeBuilder<SingleValuedPairs> b = new SampleSizeBuilder<>();
        b.setOutputFactory( outF );
        SampleSize<SingleValuedPairs> ss = b.build();

        assertTrue( Objects.nonNull( ss.getDataFactory() ) );
    }     
    
    /**
     * Checks for an expected exception on building an {@link OrdinaryScore} with a null builder.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptionOnNullBuilder() throws MetricParameterException
    {
        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Cannot construct the metric with a null builder." );

        class MockOrdinaryScore extends OrdinaryScore<SingleValuedPairs, DoubleScoreOutput>
        {

            protected MockOrdinaryScore( OrdinaryScoreBuilder<SingleValuedPairs, DoubleScoreOutput> builder )
                    throws MetricParameterException
            {
                super( null );
            }

            @Override
            public boolean isDecomposable()
            {
                return false;
            }

            @Override
            public boolean isSkillScore()
            {
                return false;
            }

            @Override
            public ScoreOutputGroup getScoreOutputGroup()
            {
                return null;
            }

            @Override
            public DoubleScoreOutput apply( SingleValuedPairs s )
            {
                return null;
            }

            @Override
            public MetricConstants getID()
            {
                return null;
            }

            @Override
            public boolean hasRealUnits()
            {
                return false;
            }
        }
        
        // Throws exception to test
        new MockOrdinaryScore( null );
    }
    
    /**
     * Checks for an expected exception on building an {@link OrdinaryScore} with a null data factory.
     * 
     * @throws MetricParameterException if the metric could not be constructed
     */

    @Test
    public void testExceptions() throws MetricParameterException
    {
        SampleSizeBuilder<SingleValuedPairs> b = new SampleSize.SampleSizeBuilder<>();

        exception.expect( MetricParameterException.class );
        exception.expectMessage( "Specify a data factory with which to build the metric." );
        
        b.build();

    }


}
